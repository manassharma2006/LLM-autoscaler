import os
import asyncio
import random
import statistics
import time
import httpx

API_URL = os.getenv("API_URL", "http://127.0.0.1:8000")
TOTAL_REQUESTS = int(os.getenv("TOTAL_REQUESTS", "24"))
CONCURRENCY = int(os.getenv("CONCURRENCY", "12"))
POLL_INTERVAL_SECONDS = 0.5
TIMEOUT_SECONDS = 180.0

FAST_PROMPTS = [
    ("Define autoscaling in one sentence.", 140),
    ("What is Redis BRPOPLPUSH used for?", 140),
    ("List three benefits of containerization.", 140),
    ("Explain latency vs throughput briefly.", 140),
]

HEAVY_PROMPTS = [
    ("Compare reactive threshold-based autoscaling with fuzzy-logic predictive autoscaling in distributed LLM inference systems, detailing latency trade-offs and cold-start overhead.", 350),
    ("Design a fault-tolerant Redis queue architecture for heterogeneous GPU workers, explaining lease recovery, heartbeat monitoring, and backpressure.", 350),
]


async def submit_and_wait(client: httpx.AsyncClient, semaphore: asyncio.Semaphore, index: int) -> dict:
    async with semaphore:
        is_heavy = (index % 4 == 0)
        prompt, max_tokens = random.choice(HEAVY_PROMPTS if is_heavy else FAST_PROMPTS)
        started = time.perf_counter()

        resp = await client.post(
            f"{API_URL}/generate",
            json={"prompt": prompt, "max_tokens": max_tokens},
        )
        resp.raise_for_status()
        payload = resp.json()
        request_id = payload["request_id"]
        assigned_tier = payload["tier"]

        deadline = started + TIMEOUT_SECONDS
        while time.perf_counter() < deadline:
            res = await client.get(f"{API_URL}/result/{request_id}")
            res.raise_for_status()
            data = res.json()
            if data["status"] in ("completed", "failed"):
                data["client_roundtrip_seconds"] = time.perf_counter() - started
                data["assigned_tier"] = assigned_tier
                ans = (data.get("output") or data.get("error") or "").replace("\n", " ")[:110]
                print(f"  -> [{assigned_tier.upper():5s} | {(data.get('inference_seconds') or 0):.2f}s] {prompt[:38]}... => {ans}")
                return data
            await asyncio.sleep(POLL_INTERVAL_SECONDS)

        return {"request_id": request_id, "status": "timeout", "assigned_tier": assigned_tier}


async def main():
    async with httpx.AsyncClient(timeout=30.0) as client:
        health = (await client.get(f"{API_URL}/health")).json()
        print("Health:", health)
        workers_before = (await client.get(f"{API_URL}/workers")).json()["workers"]
        print(f"Workers before: {len(workers_before)} -> {[w['worker_id'] for w in workers_before]}")

        semaphore = asyncio.Semaphore(CONCURRENCY)
        started = time.perf_counter()
        results = await asyncio.gather(
            *[submit_and_wait(client, semaphore, i) for i in range(TOTAL_REQUESTS)]
        )
        wall_seconds = time.perf_counter() - started

        workers_after = (await client.get(f"{API_URL}/workers")).json()["workers"]
        autoscaler = (await client.get(f"{API_URL}/autoscaler/status")).json()

    completed = [r for r in results if r.get("status") == "completed"]
    failed = [r for r in results if r.get("status") != "completed"]

    print(f"\nCompleted: {len(completed)}/{len(results)} | Failed: {len(failed)} | Wall time: {wall_seconds:.2f}s")
    for tier in ("fast", "heavy"):
        tier_res = [r for r in completed if r.get("assigned_tier") == tier]
        if tier_res:
            inf = [r["inference_seconds"] for r in tier_res if r.get("inference_seconds") is not None]
            wait = [r["queue_wait_seconds"] for r in tier_res if r.get("queue_wait_seconds") is not None]
            rt = [r["client_roundtrip_seconds"] for r in tier_res]
            print(
                f"[{tier.upper():5s}] count={len(tier_res)} | model={tier_res[0].get('model')} | "
                f"avg_wait={statistics.mean(wait):.2f}s | avg_inf={statistics.mean(inf):.2f}s | "
                f"p95_rt={sorted(rt)[max(0, int(len(rt)*0.95)-1)]:.2f}s"
            )

    print(f"Peak/Current active workers: {len(workers_after)}")
    for tier_name, info in autoscaler.get("tiers", {}).items():
        print(f"Autoscaler [{tier_name}]: {info.get('last_decision')}")


if __name__ == "__main__":
    asyncio.run(main())
