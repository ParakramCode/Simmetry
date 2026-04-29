from fastapi import APIRouter
from pydantic import BaseModel
from typing import List, Dict, Optional
from .engine import RaceEngineerAgent

router = APIRouter(prefix="/agent", tags=["AI Race Engineer"])
agent = RaceEngineerAgent()

class ChatRequest(BaseModel):
    message: str
    history: Optional[List[Dict[str, str]]] = None

class ChatResponse(BaseModel):
    response: str

@router.post("/chat", response_model=ChatResponse)
async def chat_with_engineer(request: ChatRequest):
    """
    Send a message to the AI Race Engineer.
    The agent will automatically query telemetry tools to provide data-backed answers.
    """
    response_text = agent.chat(user_message=request.message, chat_history=request.history)
    return ChatResponse(response=response_text)
