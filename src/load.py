"""Sends a steady stream of requests to the service so the Grafana panels have data.

Run:  poetry run python -m src.load                 # 5 req/s until Ctrl+C
      poetry run python -m src.load --rate 10 --seconds 60

Break / fix the (mock) LLM while it runs to watch the metrics and circuit state change:
      curl.exe -X PUT http://localhost:9000/mode/503
      curl.exe -X PUT http://localhost:9000/mode/ok
"""
import argparse
import asyncio
import time
from collections import Counter

import httpx


async def main(url: str, rate: float, seconds: float | None) -> None:
    statuses: Counter[int] = Counter()
    tasks: set[asyncio.Task] = set()
    end = time.monotonic() + seconds if seconds else float("inf")

    async with httpx.AsyncClient(base_url=url, timeout=150) as client:
        async def one() -> None:
            try:
                r = await client.post("/generate", json={"prompt": "Say hi"})
                statuses[r.status_code] += 1
            except httpx.HTTPError as e:
                print(f"request failed: {type(e).__name__}")

        last_report = time.monotonic()
        while time.monotonic() < end:
            task = asyncio.create_task(one())  # don't wait, so a slow LLM doesn't lower the rate
            tasks.add(task)
            task.add_done_callback(tasks.discard)
            await asyncio.sleep(1 / rate)
            if time.monotonic() - last_report >= 5:
                circuit = (await client.get("/circuit")).json()["state"]
                print(f"responses so far: {dict(statuses)} | circuit {circuit}")
                last_report = time.monotonic()
        await asyncio.gather(*tasks)
    print(f"done: {dict(statuses)}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default="http://localhost:8080")
    p.add_argument("--rate", type=float, default=5, help="requests per second")
    p.add_argument("--seconds", type=float, help="stop after this long (default: run until Ctrl+C)")
    args = p.parse_args()
    try:
        asyncio.run(main(args.url, args.rate, args.seconds))
    except KeyboardInterrupt:
        pass
