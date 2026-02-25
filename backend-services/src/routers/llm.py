"""
简单的 LLM 聊天接口，供 Node.js 后端调用。
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional

from openai import OpenAI
from ..config import MOONSHOT_API_KEY, MOONSHOT_BASE_URL

router = APIRouter()


class ChatRequest(BaseModel):
    prompt: str
    max_tokens: Optional[int] = 1000
    model: Optional[str] = "moonshot-v1-8k"


@router.post("/llm/chat")
def llm_chat(body: ChatRequest):
    if not MOONSHOT_API_KEY:
        raise HTTPException(503, "MOONSHOT_API_KEY not configured")
    
    client = OpenAI(api_key=MOONSHOT_API_KEY, base_url=MOONSHOT_BASE_URL)
    try:
        resp = client.chat.completions.create(
            model=body.model or "moonshot-v1-8k",
            messages=[{"role": "user", "content": body.prompt}],
            max_tokens=body.max_tokens or 1000,
        )
        content = (resp.choices[0].message.content or "").strip()
        return {"ok": True, "content": content}
    except Exception as e:
        raise HTTPException(500, f"LLM call failed: {e}")
