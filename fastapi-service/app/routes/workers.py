
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services.worker_registry import worker_registry

router = APIRouter(prefix="/workers", tags=["Workers"])


class HeartbeatRequest(BaseModel):
    active_requests: int = Field(default=0, ge=0)
    completed_requests: int = Field(default=0, ge=0)
    total_inference_seconds: float = Field(default=0.0, ge=0)
    last_inference_seconds: float = Field(default=0.0, ge=0)


@router.get("")
async def list_workers():
    return {"workers": await worker_registry.list_workers()}


@router.get("/{worker_id}")
async def get_worker(worker_id: str):
    worker = await worker_registry.get_worker(worker_id)

    if worker is None:
        raise HTTPException(status_code=404, detail="Worker not found")

    return worker


@router.post("/{worker_id}/heartbeat")
async def worker_heartbeat(
    worker_id: str,
    heartbeat: HeartbeatRequest,
):
    try:
        await worker_registry.heartbeat(
            worker_id=worker_id,
            completed_requests=heartbeat.completed_requests,
            total_inference_seconds=heartbeat.total_inference_seconds,
            last_inference_seconds=heartbeat.last_inference_seconds,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    return {"worker_id": worker_id, "accepted": True}
