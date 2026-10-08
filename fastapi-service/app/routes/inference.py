import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException

from app.models.schemas import (
    GenerateRequest,
    GenerateResponse,
    QueueStatusResponse,
)
from app.services.redis_service import redis_service
from app.config import settings


router = APIRouter()


@router.post("/generate", response_model=GenerateResponse)
async def generate(request: GenerateRequest):

    request_id = str(uuid.uuid4())

    request_data = {
        "request_id": request_id,
        "prompt": request.prompt,
        "max_tokens": request.max_tokens,
        "temperature": request.temperature,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    try:
        await redis_service.enqueue_request(request_data)

    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"Unable to queue request: {str(e)}"
        )

    return GenerateResponse(
        request_id=request_id,
        status="queued",
    )


@router.get("/queue/status", response_model=QueueStatusResponse)
async def queue_status():

    try:
        depth = await redis_service.get_queue_depth()

    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"Unable to read queue: {str(e)}"
        )

    return QueueStatusResponse(
        queue_name=settings.REDIS_QUEUE,
        queue_depth=depth,
    )
