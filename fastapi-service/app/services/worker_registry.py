
import json
import time

from app.config import settings


WORKER_IDS_KEY = "workers:ids"
HEARTBEAT_TIMEOUT_SECONDS = 15


class WorkerRegistry:
    def __init__(self):
        # Reuse the same Redis client as the existing application.
        from app.services.redis_service import redis_service
        self.redis = redis_service.client

    @staticmethod
    def worker_key(worker_id: str) -> str:
        return f"worker:{worker_id}"

    async def register(self, worker_id: str, tier: str = "fast", model: str = "unknown"):
        now = time.time()
        key = self.worker_key(worker_id)

        await self.redis.sadd(WORKER_IDS_KEY, worker_id)
        await self.redis.hset(
            key,
            mapping={
                "worker_id": worker_id,
                "tier": tier,
                "model": model,
                "status": "healthy",
                "active_requests": 0,
                "completed_requests": 0,
                "total_inference_seconds": 0.0,
                "last_inference_seconds": 0.0,
                "last_heartbeat": now,
                "registered_at": now,
            },
        )

    async def heartbeat(
        self,
        worker_id: str,
        completed_requests: int,
        total_inference_seconds: float,
        last_inference_seconds: float,
    ):
        now = time.time()
        key = self.worker_key(worker_id)

        # Do not silently recreate a worker that was never registered.
        if not await self.redis.sismember(WORKER_IDS_KEY, worker_id):
            raise ValueError(f"Worker '{worker_id}' is not registered")

        await self.redis.hset(
            key,
            mapping={
                "status": "healthy",
                "completed_requests": completed_requests,
                "total_inference_seconds": total_inference_seconds,
                "last_inference_seconds": last_inference_seconds,
                "last_heartbeat": now,
            },
        )

    async def start_request(self, worker_id: str):
        """Atomically record that a registered worker started work."""
        if not await self.redis.sismember(WORKER_IDS_KEY, worker_id):
            raise ValueError(f"Worker '{worker_id}' is not registered")
        await self.redis.hincrby(self.worker_key(worker_id), "active_requests", 1)
        await self.redis.hset(
            self.worker_key(worker_id),
            mapping={"status": "healthy", "last_heartbeat": time.time()},
        )

    async def finish_request(self, worker_id: str):
        """Decrement safely, so failures and cancellation cannot go negative."""
        key = self.worker_key(worker_id)
        result = await self.redis.eval(
            "local value = redis.call('HINCRBY', KEYS[1], ARGV[1], -1) "
            "if value < 0 then redis.call('HSET', KEYS[1], ARGV[1], 0) "
            "value = 0 end "
            "redis.call('HSET', KEYS[1], 'last_heartbeat', ARGV[2]) "
            "return value",
            1,
            key,
            "active_requests",
            time.time(),
        )
        return int(result)

    @staticmethod
    def processing_queue(worker_id: str) -> str:
        return f"{settings.REDIS_QUEUE}:processing:{worker_id}"

    async def deregister(self, worker_id: str):
        """Remove a cleanly stopped worker without touching request data."""
        processing_queue = self.processing_queue(worker_id)
        if await self.redis.llen(processing_queue):
            raise RuntimeError("cannot deregister worker with claimed requests")
        await self.redis.srem(WORKER_IDS_KEY, worker_id)
        await self.redis.delete(self.worker_key(worker_id))

    async def recover_claimed_requests(self, worker_id: str):
        """Return a stale worker's in-flight claims to the shared queue."""
        processing_queue = self.processing_queue(worker_id)
        worker_tier = await self.redis.hget(self.worker_key(worker_id), "tier") or "fast"
        target_queue = f"{settings.REDIS_QUEUE}:{worker_tier}"
        recovered = 0
        while True:
            raw_request = await self.redis.rpoplpush(
                processing_queue, target_queue
            )
            if raw_request is None:
                break
            try:
                request_id = json.loads(raw_request)["request_id"]
                request_key = f"request:{request_id}"
                status = await self.redis.hget(request_key, "status")
                if status in {"completed", "failed"}:
                    await self.redis.lrem(target_queue, 1, raw_request)
                    continue
                await self.redis.hset(
                    request_key,
                    mapping={"status": "queued", "requeued_at": time.time()},
                )
            except (KeyError, TypeError, json.JSONDecodeError):
                pass
            recovered += 1
        return recovered

    async def get_worker(self, worker_id: str):
        data = await self.redis.hgetall(self.worker_key(worker_id))
        if not data:
            return None

        last_heartbeat = float(data["last_heartbeat"])
        age = max(0.0, time.time() - last_heartbeat)

        completed = int(data["completed_requests"])
        total_time = float(data["total_inference_seconds"])

        # Completed requests per second over the worker's observed history.
        registered_at = float(data["registered_at"])
        elapsed = max(time.time() - registered_at, 1.0)
        throughput = completed / elapsed

        data.update({
            "status": (
                "healthy"
                if age <= HEARTBEAT_TIMEOUT_SECONDS
                else "unhealthy"
            ),
            "heartbeat_age_seconds": round(age, 3),
            "throughput_requests_per_second": round(throughput, 4),
            "average_inference_seconds": (
                round(total_time / completed, 4)
                if completed else None
            ),
        })

        # Convert numeric fields for a consistent API response.
        for field in (
            "active_requests",
            "completed_requests",
            "last_heartbeat",
            "registered_at",
        ):
            data[field] = int(float(data[field]))

        for field in (
            "total_inference_seconds",
            "last_inference_seconds",
        ):
            data[field] = float(data[field])

        return data


    async def cleanup_stale_workers(self):
        """Remove stale registry entries only when their work is drained."""
        worker_ids = await self.redis.smembers(WORKER_IDS_KEY)
        removed = []

        for worker_id in worker_ids:
            key = self.worker_key(worker_id)
            data = await self.redis.hgetall(key)

            if not data:
                await self.redis.srem(WORKER_IDS_KEY, worker_id)
                removed.append(worker_id)
                continue

            last_heartbeat = float(data.get("last_heartbeat", 0))
            heartbeat_age = time.time() - last_heartbeat

            if heartbeat_age <= HEARTBEAT_TIMEOUT_SECONDS:
                continue

            # Recheck the heartbeat before removing the registry entry.
            latest_heartbeat = await self.redis.hget(
                key, "last_heartbeat"
            )
            if latest_heartbeat != data.get("last_heartbeat"):
                continue

            recovered = await self.recover_claimed_requests(worker_id)
            if recovered:
                print(
                    f"[Registry] recovered {recovered} request(s) from "
                    f"stale worker {worker_id}",
                    flush=True,
                )

            await self.redis.srem(WORKER_IDS_KEY, worker_id)
            await self.redis.delete(key)
            removed.append(worker_id)

        return removed


    
    async def list_workers(self):
        await self.cleanup_stale_workers()

        worker_ids = sorted(await self.redis.smembers(WORKER_IDS_KEY))
        workers = []

        for worker_id in worker_ids:
            worker = await self.get_worker(worker_id)
            if worker is not None:
                workers.append(worker)

        return workers



worker_registry = WorkerRegistry()
