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

    async def ping(self):
        return await self.client.ping()

    async def enqueue_request(self, request_data: dict):
        await self.client.rpush(
            settings.REDIS_QUEUE,
            json.dumps(request_data)
        )

    async def get_queue_depth(self):
        return await self.client.llen(settings.REDIS_QUEUE)

    async def close(self):
        await self.client.aclose()


redis_service = RedisService()