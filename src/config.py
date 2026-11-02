import os
import logging

LLM_API_URL = os.getenv("LLM_API_URL", "http://localhost:11434")  # Ollama default
LLM_API_KEY = os.getenv("LLM_API_KEY", "")  # Ollama ignores it; the mock checks it
LLM_MODEL = os.getenv("LLM_MODEL", "llama3.2:1b")
# Caps the reply length so a rambling answer can't run past LLM_TIMEOUT
LLM_MAX_TOKENS = int(os.getenv("LLM_MAX_TOKENS", "256"))
RETRY_MAX_ATTEMPTS = int(os.getenv("RETRY_MAX_ATTEMPTS", "3"))
RETRY_INITIAL_DELAY = float(os.getenv("RETRY_INITIAL_DELAY", "1"))
RETRY_MULTIPLIER = float(os.getenv("RETRY_MULTIPLIER", "2"))
LLM_TIMEOUT = float(os.getenv("LLM_TIMEOUT", "30"))

RETRYABLE_STATUS_CODES = {429, 503}

# Circuit breaker: OPEN after CB_FAILURE_THRESHOLD failures in the last
# CB_WINDOW_SIZE calls, then HALF_OPEN after CB_OPEN_WAIT seconds.
CB_WINDOW_SIZE = int(os.getenv("CB_WINDOW_SIZE", "10"))
CB_FAILURE_THRESHOLD = int(os.getenv("CB_FAILURE_THRESHOLD", "5"))
CB_OPEN_WAIT = float(os.getenv("CB_OPEN_WAIT", "10"))
 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("llm_service")