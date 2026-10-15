import httpx
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from .llm_api import call_llm

app = FastAPI()
 
 
class GenerateRequest(BaseModel):
    prompt: str
 
 
@app.post("/generate")
async def generate(req: GenerateRequest):
    try:
        return {"response": await call_llm(req.prompt)}
    except httpx.HTTPStatusError as e:
        # Non-retryable upstream error, or a retryable one that ran out of attempts
        code = e.response.status_code
        return JSONResponse(status_code=code if code in (401, 403, 429, 503) else 502,
                            content={"error": f"LLM returned {code}"})
    except httpx.TimeoutException:
        return JSONResponse(status_code=504, content={"error": "LLM call timed out"})
    except httpx.TransportError as e:
        # Endpoint unreachable after all retries - fail gracefully, don't hang
        return JSONResponse(status_code=503, content={"error": f"LLM unreachable: {e}"})