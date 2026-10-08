from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.routes.inference import router
from app.services.redis_service import redis_service


@asynccontextmanager
async def lifespan(app: FastAPI):

    # Startup
    try:
        await redis_service.ping()
        print("Connected to Redis")
    except Exception as e:
        print(f"Redis connection failed: {e}")

    yield

    # Shutdown
    await redis_service.close()


app = FastAPI(
    title="AI Inference Gateway",
    description="FastAPI gateway for the auto-scaling inference platform",
    version="0.1.0",
    lifespan=lifespan,
)


app.include_router(router)


@app.get("/health")
async def health():
    try:
        await redis_service.ping()

        return {
            "status": "healthy",
            "redis": "connected",
        }

    except Exception:
        return {
            "status": "unhealthy",
            "redis": "disconnected",
        }
