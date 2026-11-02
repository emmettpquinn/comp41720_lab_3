"""Fake LLM endpoint for testing the retry policy and circuit breaker.

Ollama never returns 429/503 and has no API key, so this mock lets us force
those cases. The behaviour is picked by a path prefix, so one running mock can
serve every scenario:

    /ok/api/generate          -> 200 (after a short random delay, like a real model)
    /429/api/generate         -> always 429 Too Many Requests
    /503/api/generate         -> always 503 Service Unavailable
    /flaky/api/generate       -> 503 twice, then 200 (resets after success)
    /auth/api/generate        -> 401 unless "Authorization: Bearer valid-key"
    /live/api/generate        -> whatever mode was last set with PUT /mode/{mode}

"live" lets you break and fix the LLM while the service keeps running:

    curl.exe -X PUT http://localhost:9000/mode/503   # start failing
    curl.exe -X PUT http://localhost:9000/mode/ok    # recover
    curl.exe http://localhost:9000/calls             # how many requests reached the mock

Run:  poetry run uvicorn src.mock_llm:app --port 9000
"""
import asyncio
import random

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

app = FastAPI()
_flaky_calls = 0
_live_mode = "ok"
_total_calls = 0


@app.put("/mode/{mode}")
async def set_mode(mode: str):
    global _live_mode
    _live_mode = mode
    return {"live_mode": _live_mode}


@app.get("/calls")
async def calls():
    return {"total_calls": _total_calls, "live_mode": _live_mode}


@app.post("/{mode}/api/generate")
async def generate(mode: str, request: Request):
    global _flaky_calls, _total_calls
    _total_calls += 1
    if mode == "live":
        mode = _live_mode
    if mode == "429":
        return JSONResponse(status_code=429, content={"error": "rate limited"})
    if mode == "503":
        return JSONResponse(status_code=503, content={"error": "overloaded"})
    if mode == "flaky":
        _flaky_calls += 1
        if _flaky_calls <= 2:
            return JSONResponse(status_code=503, content={"error": "overloaded"})
        _flaky_calls = 0
    if mode == "auth" and request.headers.get("authorization") != "Bearer valid-key":
        return JSONResponse(status_code=401, content={"error": "invalid API key"})
    await asyncio.sleep(random.uniform(0.05, 0.4))
    return {"response": f"mock reply ({mode})"}
