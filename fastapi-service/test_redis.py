
import asyncio
import redis.asyncio as redis


async def main():
    client = redis.Redis(
        host="localhost",
        port=6379,
        db=0,
        decode_responses=True,
        socket_connect_timeout=3,
        socket_timeout=3,
    )

    try:
        for i in range(5):
            print(f"Test {i + 1}: {await client.ping()}")
            await asyncio.sleep(1)
    except Exception as exc:
        print(type(exc).__name__, repr(exc))
    finally:
        await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())


