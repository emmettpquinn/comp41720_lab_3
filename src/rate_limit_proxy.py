"""Rate-limiting reverse proxy in front of Ollama (chaos Tests 2 and 3).

Ollama never answers 429 - it just queues requests. This proxy forwards to
Ollama but only lets MAX_CONCURRENT requests be in flight at once; any request
beyond that gets an immediate 429 Too Many Requests, like a cloud LLM API would.

The service is pointed at http://localhost:9999 for all chaos tests:
  - nothing listening on 9999   -> LLM "down"              (Test 1)
  - this proxy on 9999          -> LLM up, but rate limited (Test 2)
  - this proxy, high cap        -> LLM restored             (Test 3)

Run:  poetry run uvicorn src.rate_limit_proxy:app --port 9999
      (env: UPSTREAM_URL default http://localhost:11434, MAX_CONCURRENT default 2)
"""
import os

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from .config import log

UPSTREAM_URL = os.getenv("UPSTREAM_URL", "http://localhost:11434")
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "2"))

app = FastAPI()
_client = httpx.AsyncClient(base_url=UPSTREAM_URL, timeout=None)
_in_flight = 0


@app.post("/api/generate")
async def generate(request: Request):
    global _in_flight
    # No await between the check and the increment, so this is race-free on one event loop
    if _in_flight >= MAX_CONCURRENT:
        log.warning("PROXY: %d/%d in flight - 429 Too Many Requests", _in_flight, MAX_CONCURRENT)
        return JSONResponse(status_code=429, content={"error": "rate limited"}, headers={"Retry-After": "1"})
    _in_flight += 1
    try:
        resp = await _client.post("/api/generate", content=await request.body(),
                                  headers={"Content-Type": "application/json"})
        return Response(resp.content, status_code=resp.status_code, media_type="application/json")
    finally:
        _in_flight -= 1
