import asyncio
import json
import os
import signal
import socket
import time
from datetime import datetime

import redis.asyncio as redis

from app.config import settings
from app.services.worker_registry import worker_registry


class InferenceWorker:
    def __init__(self):
        self.worker_id = os.getenv("WORKER_ID") or f"worker-{socket.gethostname()}"
        self.completed_requests = 0
        self.total_inference_seconds = 0.0
        self.last_inference_seconds = 0.0
        self.redis = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=10,
            health_check_interval=30,
        )
        self.queue_name = settings.REDIS_QUEUE
        self.processing_queue = worker_registry.processing_queue(self.worker_id)
        self.stop_requested = asyncio.Event()

    async def send_heartbeat(self):
        await worker_registry.heartbeat(
            worker_id=self.worker_id,
            completed_requests=self.completed_requests,
            total_inference_seconds=self.total_inference_seconds,
            last_inference_seconds=self.last_inference_seconds,
        )

    async def heartbeat_loop(self):
        while not self.stop_requested.is_set():
            try:
                await self.send_heartbeat()
            except Exception as exc:
                print(f"[{self.worker_id}] heartbeat error: {exc}", flush=True)
            try:
                await asyncio.wait_for(self.stop_requested.wait(), timeout=5)
            except asyncio.TimeoutError:
                pass

    async def generate(self, prompt: str, max_tokens: int) -> str:
        """Temporary simulated inference backend for the demo."""
        await asyncio.sleep(2)
        return (
            f"[SIMULATED OUTPUT] Processed prompt: {prompt[:200]} "
            f"(max_tokens={max_tokens})"
        )

    @staticmethod
    def created_timestamp(value: str) -> float:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()

    async def requeue_claim(self, raw_request: str, request_id: str | None = None):
        """Put an unfinished claim back before this worker exits."""
        await self.redis.lrem(self.processing_queue, 1, raw_request)
        await self.redis.lpush(self.queue_name, raw_request)
        if request_id:
            await self.redis.hset(
                f"request:{request_id}",
                mapping={"status": "queued", "requeued_at": time.time()},
            )

    async def process_claim(self, raw_request: str):
        request = json.loads(raw_request)
        request_id = request["request_id"]
        request_key = f"request:{request_id}"
        terminal = False
        started = False

        try:
            started_at = time.time()
            queue_wait = max(0.0, started_at - self.created_timestamp(request["created_at"]))
            await worker_registry.start_request(self.worker_id)
            started = True
            await self.redis.hset(
                request_key,
                mapping={
                    "status": "processing",
                    "worker_id": self.worker_id,
                    "started_at": started_at,
                    "queue_wait_seconds": queue_wait,
                },
            )
            print(f"[{self.worker_id}] processing request={request_id}", flush=True)

            inference_started = time.time()
            output = await self.generate(request["prompt"], int(request["max_tokens"]))
            finished_at = time.time()
            duration = finished_at - inference_started
            self.completed_requests += 1
            self.total_inference_seconds += duration
            self.last_inference_seconds = duration
            await self.redis.hset(
                request_key,
                mapping={
                    "status": "completed",
                    "output": output,
                    "finished_at": finished_at,
                    "inference_seconds": duration,
                    "total_seconds": max(0.0, finished_at - self.created_timestamp(request["created_at"])),
                },
            )
            terminal = True
            print(f"[{self.worker_id}] completed request={request_id}", flush=True)
        except asyncio.CancelledError:
            await self.requeue_claim(raw_request, request_id)
            raise
        except Exception as exc:
            finished_at = time.time()
            await self.redis.hset(
                request_key,
                mapping={
                    "status": "failed",
                    "error": str(exc)[:1000],
                    "finished_at": finished_at,
                    "total_seconds": max(0.0, finished_at - self.created_timestamp(request["created_at"])),
                },
            )
            terminal = True
            print(f"[{self.worker_id}] failed request={request_id}: {exc}", flush=True)
        finally:
            if started:
                await worker_registry.finish_request(self.worker_id)
            if terminal:
                await self.redis.lrem(self.processing_queue, 1, raw_request)

    async def run(self):
        print(f"[{self.worker_id}] listening on shared queue {self.queue_name}", flush=True)
        await worker_registry.register(self.worker_id)
        heartbeat_task = asyncio.create_task(self.heartbeat_loop())

        try:
            while not self.stop_requested.is_set():
                raw_request = await self.redis.brpoplpush(
                    self.queue_name, self.processing_queue, timeout=1
                )
                if raw_request is None:
                    continue
                await self.process_claim(raw_request)
        finally:
            self.stop_requested.set()
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass
            try:
                await worker_registry.deregister(self.worker_id)
            except Exception as exc:
                print(f"[{self.worker_id}] deregistration deferred: {exc}", flush=True)

    async def close(self):
        await self.redis.aclose()


async def main():
    worker = InferenceWorker()
    loop = asyncio.get_running_loop()

    def request_stop():
        print(f"[{worker.worker_id}] stop requested; draining current request", flush=True)
        worker.stop_requested.set()

    for signal_name in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(signal_name, request_stop)
    try:
        await worker.run()
    finally:
        await worker.close()


if __name__ == "__main__":
    asyncio.run(main())
