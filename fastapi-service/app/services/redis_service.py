
import json

import redis.asyncio as redis

from app.config import settings


class RedisService:
    def __init__(self):
        self.client = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            decode_responses=True,
        )

    def request_key(self, request_id: str) -> str:
        return f"request:{request_id}"

    async def ping(self):
        return await self.client.ping()

    async def enqueue_request(self, request_data: dict):
        request_id = request_data["request_id"]
        queue_name = settings.REDIS_QUEUE

        async with self.client.pipeline(transaction=True) as pipeline:
            pipeline.hset(
                self.request_key(request_id),
                mapping={
                    "request_id": request_id,
                    "status": "queued",
                    "prompt": request_data["prompt"],
                    "max_tokens": request_data["max_tokens"],
                    "temperature": request_data["temperature"],
                    "created_at": request_data["created_at"],
                },
            )
            pipeline.lpush(queue_name, json.dumps(request_data))
            await pipeline.execute()

    async def get_queue_depth(self):
        return await self.client.llen(settings.REDIS_QUEUE)

    async def get_request(self, request_id: str):
        data = await self.client.hgetall(self.request_key(request_id))
        return data or None

    async def update_request(self, request_id: str, fields: dict):
        clean_fields = {
            key: str(value)
            for key, value in fields.items()
            if value is not None
        }
        if clean_fields:
            await self.client.hset(
                self.request_key(request_id),
                mapping=clean_fields,
            )

    async def close(self):
        await self.client.aclose()


redis_service = RedisService()
