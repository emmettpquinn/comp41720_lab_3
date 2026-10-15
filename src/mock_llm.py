"""Fake LLM endpoint for testing the retry policy.

Ollama never returns 429/503 and has no API key, so this mock lets us force
those cases. The behaviour is picked by a path prefix, so one running mock can
serve every scenario:

    /ok/api/generate          -> 200
    /429/api/generate         -> always 429 Too Many Requests
    /503/api/generate         -> always 503 Service Unavailable
    /flaky/api/generate       -> 503 twice, then 200 (per client, resets after success)
    /auth/api/generate        -> 401 unless "Authorization: Bearer valid-key"

Run:  poetry run uvicorn src.mock_llm:app --port 9000
"""
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

app = FastAPI()
_flaky_calls = 0


@app.post("/{mode}/api/generate")
async def generate(mode: str, request: Request):
    global _flaky_calls
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
    return {"response": f"mock reply ({mode})"}
