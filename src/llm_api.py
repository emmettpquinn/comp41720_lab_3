import asyncio

import httpx
from tenacity import RetryCallState, retry, retry_if_exception, stop_after_attempt, wait_exponential
from .circuit_breaker import CircuitBreaker
from .config import CB_FAILURE_THRESHOLD, CB_OPEN_WAIT, CB_WINDOW_SIZE, LLM_API_KEY, LLM_API_URL, LLM_MAX_TOKENS, LLM_MODEL, LLM_TIMEOUT, RETRY_MAX_ATTEMPTS, RETRY_INITIAL_DELAY, RETRY_MULTIPLIER, RETRYABLE_STATUS_CODES, log

# One shared client: reuses connections instead of paying setup cost on every call
_client = httpx.AsyncClient(timeout=LLM_TIMEOUT)


# --- Retry policy ------------------------------------------------------------
def is_retryable(exc: BaseException) -> bool:
    """Retry on 429/503, and on connection failures (endpoint down/unreachable).
    Anything else (401, 403, 404, 400...) fails immediately - retrying won't help."""
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in RETRYABLE_STATUS_CODES
    return isinstance(exc, httpx.ConnectError)
 
 
def describe(exc: BaseException) -> str:
    """One-line summary of an error for the logs."""
    if isinstance(exc, httpx.HTTPStatusError):
        return f"HTTP {exc.response.status_code}"
    return f"{type(exc).__name__}: {exc}"


def log_retry(state: RetryCallState) -> None:
    """Called by Tenacity after a failed attempt, before it sleeps."""
    exc = state.outcome.exception()
    log.warning(
        "Attempt %d/%d failed (%s) - retrying in %.1fs",
        state.attempt_number,
        RETRY_MAX_ATTEMPTS,
        describe(exc),
        state.next_action.sleep,
    )
 
 
def log_give_up(state: RetryCallState):
    """Called once all attempts are used up. Re-raise the original error."""
    exc = state.outcome.exception()
    log.error("Giving up after %d attempts (%s)", state.attempt_number, describe(exc))
    raise exc
 
 
# wait = initial_delay * multiplier ** (attempt - 1)  ->  1s, 2s, 4s, ...
@retry(
    retry=retry_if_exception(is_retryable),
    stop=stop_after_attempt(RETRY_MAX_ATTEMPTS),
    wait=wait_exponential(multiplier=RETRY_INITIAL_DELAY, exp_base=RETRY_MULTIPLIER, max=60),
    before_sleep=log_retry,
    retry_error_callback=log_give_up,
)
async def call_llm(prompt: str) -> str:
    headers = {"Authorization": f"Bearer {LLM_API_KEY}"} if LLM_API_KEY else {}
    log.info("Calling LLM at %s", LLM_API_URL)
    try:
        # httpx's timeout is per phase (connect/read/...), so a model that streams
        # bytes slowly could run past it. This caps the whole attempt at LLM_TIMEOUT.
        async with asyncio.timeout(LLM_TIMEOUT):
            resp = await _client.post(
                f"{LLM_API_URL}/api/generate",
                json={"model": LLM_MODEL, "prompt": prompt, "stream": False,
                      "options": {"num_predict": LLM_MAX_TOKENS}},
                headers=headers,
            )
    except TimeoutError:
        # Not retried (is_retryable only retries ConnectError): 3 x 30s would leave the
        # client waiting 90s+. Surfaces as 504 and counts as a breaker failure.
        raise httpx.TimeoutException(f"LLM did not answer within {LLM_TIMEOUT:.0f}s")
    resp.raise_for_status()  # turns 429/503 etc. into HTTPStatusError
    return resp.json()["response"]


# --- Circuit breaker ---------------------------------------------------------
def is_breaker_failure(exc: BaseException) -> bool:
    """Signs the LLM itself is unhealthy: 5xx, 429, timeouts, unreachable.
    Other 4xx (bad key, bad path) are our fault, so they don't trip the breaker."""
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        return code >= 500 or code == 429
    return isinstance(exc, httpx.TransportError)  # includes timeouts


llm_breaker = CircuitBreaker(
    "llm",
    window_size=CB_WINDOW_SIZE,
    failure_threshold=CB_FAILURE_THRESHOLD,
    open_wait=CB_OPEN_WAIT,
    is_failure=is_breaker_failure,
)


async def generate_text(prompt: str) -> str:
    """Breaker wraps retry: one request (with all its retries) = one breaker outcome."""
    return await llm_breaker.call(call_llm, prompt)
