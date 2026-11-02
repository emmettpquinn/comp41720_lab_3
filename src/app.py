import math
import time

import httpx
from fastapi import FastAPI, Response
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from pydantic import BaseModel

from .circuit_breaker import CircuitOpenError
from .config import log
from .llm_api import generate_text, llm_breaker

app = FastAPI()

# --- Prometheus metrics ------------------------------------------------------
REQUESTS = Counter("llm_requests_total", "Requests received on /generate")
ERRORS = Counter("llm_request_errors_total", "Failed /generate requests", ["reason"])
# Export every reason at 0 from the start: rate() ignores the first increment of a
# series Prometheus hasn't seen before, so a burst of new 429s would otherwise not show.
for _reason in ("circuit_open", "timeout", "unreachable", "http_401", "http_403", "http_429", "http_503"):
    ERRORS.labels(reason=_reason)
LATENCY = Histogram(
    "llm_request_duration_seconds",
    "End-to-end /generate latency, including retries",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 15, 20, 30, 60, 120),
)
CIRCUIT_STATE = Gauge("circuit_breaker_state", "Circuit breaker state: 0=CLOSED, 1=OPEN, 2=HALF_OPEN")
# Read the state at scrape time, so OPEN -> HALF_OPEN shows up even with no traffic
CIRCUIT_STATE.set_function(lambda: llm_breaker.state.value)


class GenerateRequest(BaseModel):
    prompt: str


def error(status: int, reason: str, message: str, headers: dict | None = None) -> JSONResponse:
    ERRORS.labels(reason=reason).inc()
    return JSONResponse(status_code=status, content={"error": message}, headers=headers)


@app.post("/generate")
async def generate(req: GenerateRequest):
    REQUESTS.inc()
    start = time.perf_counter()
    try:
        return {"response": await generate_text(req.prompt)}
    except CircuitOpenError as e:
        # Fallback - fail fast: the LLM was not called at all
        retry_after = max(1, math.ceil(e.retry_after))
        log.warning("FALLBACK: circuit %s, returning 503 circuit_open (retry_after=%ds) for prompt %r",
                    e.state.name, retry_after, req.prompt[:40])
        ERRORS.labels(reason="circuit_open").inc()
        return JSONResponse(
            status_code=503,
            content={
                "status": "circuit_open",
                "retry_after": retry_after,
                "message": "LLM service temporarily unavailable",
            },
            headers={"Retry-After": str(retry_after)},
        )
    except httpx.HTTPStatusError as e:
        # Non-retryable upstream error, or a retryable one that ran out of attempts
        # 401/403 (bad key) are passed through as-is; 429/503 mean retries were exhausted.
        code = e.response.status_code
        return error(code if code in (401, 403, 429, 503) else 502, f"http_{code}", f"LLM returned {code}")
    except httpx.TimeoutException:
        return error(504, "timeout", "LLM call timed out")
    except httpx.TransportError as e:
        # Endpoint unreachable after all retries - fail gracefully, don't hang
        return error(503, "unreachable", f"LLM unreachable: {e}")
    finally:
        LATENCY.observe(time.perf_counter() - start)


@app.get("/circuit")
def circuit():
    return {
        "state": llm_breaker.state.name,
        "failures_in_window": llm_breaker.failures_in_window,
        "failure_threshold": llm_breaker.failure_threshold,
    }


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


if __name__ == "__main__":
    # Port 8080 is fixed by the lab harness
    import socket

    import uvicorn

    # Listen on IPv4 and IPv6: clients resolving "localhost" try ::1 first, and on
    # Windows an IPv4-only server makes each of those requests wait ~2s for the refusal.
    if socket.has_dualstack_ipv6():
        sock = socket.create_server(("::", 8080), family=socket.AF_INET6, dualstack_ipv6=True)
        uvicorn.Server(uvicorn.Config(app)).run(sockets=[sock])
    else:
        uvicorn.run(app, host="0.0.0.0", port=8080)
