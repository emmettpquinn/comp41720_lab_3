"""Exercise 3.2 demo: walks the circuit breaker through every state transition.

Starts the mock LLM in a background thread and sends requests through the real
FastAPI app. Retries are turned off here (1 attempt) so each request is quick and
counts as exactly one success/failure in the breaker's window.

Run:  poetry run python -m src.demo_circuit
"""
import asyncio
import os
import threading
import time

os.environ.setdefault("RETRY_MAX_ATTEMPTS", "1")
os.environ.setdefault("LLM_API_URL", "http://127.0.0.1:9000/live")

import httpx  # noqa: E402
import uvicorn  # noqa: E402

from .app import app  # noqa: E402
from .config import CB_OPEN_WAIT, log  # noqa: E402
from .mock_llm import app as mock_app  # noqa: E402

MOCK = "http://127.0.0.1:9000"


def start_mock() -> None:
    server = uvicorn.Server(uvicorn.Config(mock_app, port=9000, log_level="warning"))
    threading.Thread(target=server.run, daemon=True).start()
    while not server.started:
        time.sleep(0.05)


async def main() -> None:
    start_mock()
    service = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://service")
    mock = httpx.AsyncClient(base_url=MOCK)

    async def set_mode(mode: str) -> None:
        await mock.put(f"/mode/{mode}")
        log.info("[mock LLM now returns: %s]", mode)

    async def llm_calls() -> int:
        return (await mock.get("/calls")).json()["total_calls"]

    async def send(label: str = "") -> None:
        start = time.perf_counter()
        r = await service.post("/generate", json={"prompt": "hi"})
        elapsed = (time.perf_counter() - start) * 1000
        rejected = "rejected" in r.json().get("error", "")
        state = (await service.get("/circuit")).json()["state"]
        log.info("  %s-> HTTP %d in %4.0fms | %s | circuit now %s", label, r.status_code, elapsed,
                 "REJECTED by breaker, LLM not called" if rejected else "forwarded to LLM", state)

    async def wait_for_half_open() -> None:
        log.info("[waiting %.0fs for the open timeout...]", CB_OPEN_WAIT)
        await asyncio.sleep(CB_OPEN_WAIT + 0.2)
        log.info("  circuit now %s", (await service.get("/circuit")).json()["state"])

    log.info("=== 1. CLOSED: healthy LLM, every request is forwarded")
    await set_mode("ok")
    for _ in range(3):
        await send()

    log.info("=== 2. CLOSED -> OPEN: LLM starts failing, breaker opens at 5 failures in last 10 calls")
    await set_mode("503")
    for _ in range(5):
        await send()

    log.info("=== 3. OPEN: requests are rejected immediately without calling the LLM (fail fast)")
    await set_mode("ok")  # even though the LLM has recovered, OPEN doesn't try it
    before = await llm_calls()
    for _ in range(3):
        await send()
    log.info("  mock LLM received %d requests while OPEN", await llm_calls() - before)

    log.info("=== 4. OPEN -> HALF_OPEN -> OPEN: trial request fails")
    await wait_for_half_open()
    await set_mode("503")
    await send("trial ")

    log.info("=== 5. OPEN -> HALF_OPEN -> CLOSED: trial request succeeds")
    await wait_for_half_open()
    await set_mode("ok")
    log.info("  sending 2 requests at once - only one trial is allowed through in HALF_OPEN:")
    await asyncio.gather(send("req A "), send("req B "))
    await send("after ")

    await service.aclose()
    await mock.aclose()


if __name__ == "__main__":
    asyncio.run(main())
