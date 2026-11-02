"""Task B: chaos engineering tests against the running service on :8080.

Setup (three terminals + Prometheus/Grafana):
  1. ollama serve                                   (model llama3.2:1b pulled)
  2. docker compose up -d                           (Prometheus :9090, Grafana :3000)
  3. $env:LLM_API_URL="http://localhost:9999"; poetry run python -m src.app
  4. poetry run python -m src.chaos all             (or: 1, 2, 3)

The service always points at http://localhost:9999. This script breaks and fixes
the "LLM" by stopping/starting src/rate_limit_proxy.py on that port, so the
service never needs restarting and the breaker keeps its state between tests.

EXPECTED BEHAVIOUR (defaults: 3 attempts, 1s/2s backoff, 5 failures in 10 -> OPEN,
OPEN for 10s, 30s LLM timeout):

Test 1 - LLM downtime (nothing listening on :9999)
  (a) Each request makes 3 attempts: ConnectError, wait 1s, ConnectError, wait 2s,
      ConnectError -> gives up and returns 503 {"error": "LLM unreachable"}. That is
      ~3s of backoff; on Windows each refused connect to "localhost" also takes ~2s
      per address (::1 and 127.0.0.1), so a request takes ~10s in total.
      Service log shows "Attempt 1/3 failed ... retrying in 1.0s" / "in 2.0s".
  (b) Each exhausted request is one breaker failure. The 5th opens the circuit:
      "STATE CHANGE: CLOSED -> OPEN (5 failures in last 5 calls)".
  (c) Requests 6+ return 503 {"status": "circuit_open", ...} in a few ms, the LLM
      is not called (no "Calling LLM" log line), and a FALLBACK log line is written.
  Grafana: error rate (reason=unreachable, then circuit_open) rises, p99 latency
  ~10s, circuit state 0 -> 1.

Test 2 - Rate limiting (proxy on :9999 lets only 2 requests in flight, rest 429)
  (a) Of 15 concurrent requests, 2 reach Ollama, the rest get 429 and are retried
      with backoff (log: "Attempt 1/3 failed (HTTP 429) - retrying in 1.0s").
      Some win a slot on a retry and succeed.
  (b) Requests that still get 429 on attempt 3 return 429 to the client and count
      as breaker failures; with >= 5 of them the circuit opens and the rest of the
      burst / the next burst fail fast with 503 circuit_open.
  Grafana: request-rate and error-rate spike (reason=http_429, circuit_open),
  circuit state may go 0 -> 1.

Test 3 - Recovery (proxy on :9999 with no effective cap, no flooding)
  (a) While OPEN, requests still get 503 circuit_open. After 10s the circuit goes
      HALF_OPEN ("STATE CHANGE: OPEN -> HALF_OPEN (10s wait elapsed)") and the next
      request is let through as the trial; it reaches Ollama and succeeds.
  (b) "STATE CHANGE: HALF_OPEN -> CLOSED (trial call succeeded)".
  (c) Subsequent requests return 200 with generated text at normal latency.
  Grafana: circuit state 1 -> 2 -> 0, error rate falls to 0, request rate steady.
"""
import argparse
import asyncio
import os
import socket
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path

import httpx

SERVICE = "http://localhost:8080"
PROXY_PORT = 9999
LOG_DIR = Path("logs")
_proxy: subprocess.Popen | None = None
T0 = time.monotonic()


def say(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')} +{time.monotonic() - T0:5.1f}s] {msg}", flush=True)


# --- controlling the "LLM" ----------------------------------------------------
def port_open(port: int) -> bool:
    with socket.socket() as s:
        s.settimeout(0.2)
        return s.connect_ex(("127.0.0.1", port)) == 0


def stop_proxy() -> None:
    global _proxy
    if _proxy:
        _proxy.terminate()
        _proxy.wait()
        _proxy = None
    while port_open(PROXY_PORT):
        time.sleep(0.1)
    say(f"LLM endpoint DOWN (nothing listening on :{PROXY_PORT})")


def start_proxy(max_concurrent: int) -> None:
    global _proxy
    stop_proxy()
    LOG_DIR.mkdir(exist_ok=True)
    env = {**os.environ, "MAX_CONCURRENT": str(max_concurrent)}
    _proxy = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "src.rate_limit_proxy:app", "--port", str(PROXY_PORT),
         "--log-level", "warning"],
        env=env, stdout=open(LOG_DIR / "proxy.log", "a"), stderr=subprocess.STDOUT,
    )
    while not port_open(PROXY_PORT):
        time.sleep(0.1)
    say(f"LLM endpoint UP on :{PROXY_PORT} -> Ollama, max {max_concurrent} concurrent (excess -> 429)")


# --- talking to the service ---------------------------------------------------
async def state(client: httpx.AsyncClient) -> str:
    return (await client.get("/circuit")).json()["state"]


async def send(client: httpx.AsyncClient, label: str) -> int:
    start = time.perf_counter()
    r = await client.post("/generate", json={"prompt": "Reply with one short sentence."})
    ms = (time.perf_counter() - start) * 1000
    body = r.json()
    detail = body.get("status") or body.get("error") or repr(body.get("response", ""))[:50]
    say(f"{label:<10} HTTP {r.status_code} in {ms:7.0f}ms  {detail:<45} circuit={await state(client)}")
    return r.status_code


async def wait_for_closed(client: httpx.AsyncClient) -> None:
    """Probe every 2s until a request succeeds and the breaker is CLOSED."""
    while True:
        if await send(client, "probe") == 200 and await state(client) == "CLOSED":
            return
        await asyncio.sleep(2)


# --- the tests ----------------------------------------------------------------
async def test1(client: httpx.AsyncClient) -> None:
    say("===== TEST 1: simulated LLM downtime =====")
    stop_proxy()
    say(f"circuit at start: {await state(client)}")
    for i in range(1, 9):
        await send(client, f"req {i}")


async def test2(client: httpx.AsyncClient) -> None:
    say("===== TEST 2: rate limiting (flood of concurrent requests) =====")
    start_proxy(max_concurrent=2)
    if await state(client) != "CLOSED":
        say("circuit not CLOSED yet - waiting for it to recover before flooding")
        await wait_for_closed(client)
    for burst in (1, 2):
        say(f"--- burst {burst}: 15 concurrent requests")
        codes = await asyncio.gather(*(send(client, f"b{burst}-{i:02d}") for i in range(1, 16)))
        say(f"burst {burst} result: {dict(Counter(codes))}  circuit={await state(client)}")


async def test3(client: httpx.AsyncClient) -> None:
    say("===== TEST 3: recovery =====")
    start_proxy(max_concurrent=100)  # endpoint fixed, flooding stopped
    say(f"circuit at start: {await state(client)}")
    while await state(client) == "OPEN":
        await send(client, "while OPEN")
        await asyncio.sleep(2)
    say(f"circuit is now {await state(client)} - next request is the trial")
    await send(client, "trial")
    for i in range(1, 6):
        await send(client, f"after {i}")
        await asyncio.sleep(1)


async def main(which: str) -> None:
    async with httpx.AsyncClient(base_url=SERVICE, timeout=120) as client:
        try:
            if which in ("1", "all"):
                await test1(client)
            if which in ("2", "all"):
                if which == "all":
                    await asyncio.sleep(15)  # gap so the tests are separate on the Grafana graphs
                await test2(client)
            if which in ("3", "all"):
                await test3(client)  # straight after test 2, while the circuit is still OPEN
        finally:
            if _proxy:
                _proxy.terminate()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("test", choices=["1", "2", "3", "all"])
    asyncio.run(main(p.parse_args().test))
