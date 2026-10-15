import os
import logging

LLM_API_URL = os.getenv("LLM_API_URL", "http://localhost:11434")  # Ollama default
LLM_API_KEY = os.getenv("LLM_API_KEY", "")  # Ollama ignores it; the mock checks it
LLM_MODEL = os.getenv("LLM_MODEL", "llama3.2:1b")
RETRY_MAX_ATTEMPTS = int(os.getenv("RETRY_MAX_ATTEMPTS", "3"))
RETRY_INITIAL_DELAY = float(os.getenv("RETRY_INITIAL_DELAY", "1"))
RETRY_MULTIPLIER = float(os.getenv("RETRY_MULTIPLIER", "2"))
LLM_TIMEOUT = float(os.getenv("LLM_TIMEOUT", "30"))
 
RETRYABLE_STATUS_CODES = {429, 503}
 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("llm_service")