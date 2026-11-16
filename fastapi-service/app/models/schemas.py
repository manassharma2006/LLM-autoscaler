from typing import Literal, Optional
from pydantic import BaseModel, Field


class GenerateRequest(BaseModel):
    prompt: str = Field(..., min_length=1)
    max_tokens: int = Field(default=100, ge=1, le=4096)
    temperature: float = Field(default=0.7, ge=0.0, le=2.0)
    tier: Optional[Literal["fast", "heavy"]] = None


class GenerateResponse(BaseModel):
    request_id: str
    status: str
    tier: str


class QueueStatusResponse(BaseModel):
    queue_name: str
    queue_depth: int
    queues: dict[str, int] = {}


class ResultResponse(BaseModel):
    request_id: str
    status: str
    tier: Optional[str] = None
    model: Optional[str] = None
    output: Optional[str] = None
    error: Optional[str] = None
    queue_wait_seconds: Optional[float] = None
    inference_seconds: Optional[float] = None
    total_seconds: Optional[float] = None
