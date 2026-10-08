import asyncio
import json

import redis.asyncio as redis

from app.config import settings


class InferenceWorker:

    def __init__(self):
        self.redis = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            decode_responses=True,
        )

        self.queue_name = settings.REDIS_QUEUE

    async def run(self):

        print("Worker started...")
        print(f"Listening on Redis queue: {self.queue_name}")

        while True:
            try:
                # Wait for the next request.
                # BLPOP blocks until something is available.
                result = await self.redis.blpop(
                    self.queue_name,
                    timeout=0
                )

                if result is None:
                    continue

                queue_name, raw_request = result

                request = json.loads(raw_request)

                request_id = request["request_id"]
                prompt = request["prompt"]

                print("\nReceived request")
                print(f"Request ID: {request_id}")
                print(f"Prompt: {prompt}")

                # Temporary processing simulation.
                # We will replace this with vLLM.
                await asyncio.sleep(2)

                print(f"Finished request: {request_id}")

            except Exception as e:
                print(f"Worker error: {e}")
                await asyncio.sleep(1)

    async def close(self):
        await self.redis.aclose()


async def main():

    worker = InferenceWorker()

    try:
        await worker.run()

    except KeyboardInterrupt:
        print("\nWorker shutting down...")

    finally:
        await worker.close()


if __name__ == "__main__":
    asyncio.run(main())