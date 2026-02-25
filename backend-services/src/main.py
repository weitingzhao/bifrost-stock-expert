from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .config import MOONSHOT_API_KEY
from .routers import trigger, llm

app = FastAPI(title="StEx Backend Services", version="0.1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/health")
def health():
    return {"ok": True, "service": "stex-python", "llm_configured": bool(MOONSHOT_API_KEY)}


app.include_router(trigger.router, prefix="/api", tags=["trigger"])
app.include_router(llm.router, prefix="/api", tags=["llm"])
