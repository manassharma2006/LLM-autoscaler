from pydantic import BaseModel, Field


class GenerateRequest(BaseModel):
    prompt: str = Field(..., min_length=1)
    max_tokens: int = Field(default=100, ge=1, le=4096)
    temperature: float = Field(default=0.7, ge=0.0, le=2.0)


class GenerateResponse(BaseModel):
    request_id: str
    status: str


class QueueStatusResponse(BaseModel):
    queue_name: str
    queue_depth: int
