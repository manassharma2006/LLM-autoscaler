
from fastapi import APIRouter, HTTPException

from app.services.autoscaler import autoscaler_service

router = APIRouter(prefix="/autoscaler", tags=["Autoscaler"])


@router.get("/status")
async def get_autoscaler_status():
    return autoscaler_service.status()


@router.post("/evaluate")
async def evaluate_autoscaler():
    try:
        return await autoscaler_service.evaluate()
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Autoscaler evaluation failed: {exc}",
        )
