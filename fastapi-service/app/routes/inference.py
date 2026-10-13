import uuid
from datetime import datetime, timezone
from fastapi import APIRouter, HTTPException

from app.models.schemas import (
    GenerateRequest,
    GenerateResponse,
    QueueStatusResponse,
    ResultResponse,
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

    # This call stores the request and enqueues it once on the shared queue.
    await redis_service.enqueue_request(request_data)


    return GenerateResponse(
        request_id=request_id,
        status="queued",
    )


@router.get("/result/{request_id}", response_model=ResultResponse)
async def get_result(request_id: str):
    try:
        data = await redis_service.get_request(request_id)
    except Exception:
        raise HTTPException(
            status_code=503,
            detail="Unable to read request status. Check Redis.",
        )

    if not data:
        raise HTTPException(
            status_code=404,
            detail="Request not found.",
        )

    def parse_float(value):
        return float(value) if value is not None else None

    return ResultResponse(
        request_id=request_id,
        status=data["status"],
        output=data.get("output"),
        error=data.get("error"),
        queue_wait_seconds=parse_float(
            data.get("queue_wait_seconds")
        ),
        inference_seconds=parse_float(
            data.get("inference_seconds")
        ),
        total_seconds=parse_float(data.get("total_seconds")),
    )


@router.get("/queue/status", response_model=QueueStatusResponse)
async def queue_status():
    try:
        depth = await redis_service.get_queue_depth()
    except Exception:
        raise HTTPException(
            status_code=503,
            detail="Unable to read queue. Check Redis.",
        )

    return QueueStatusResponse(
        queue_name=settings.REDIS_QUEUE,
        queue_depth=depth,
    )
