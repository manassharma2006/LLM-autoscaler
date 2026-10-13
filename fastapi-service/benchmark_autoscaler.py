import asyncio
import statistics
import time

import httpx

BASE_URL = "http://127.0.0.1:8000"
TOTAL_REQUESTS = 50
CONCURRENCY = 50
COMPLETION_TIMEOUT_SECONDS = 180
POLL_INTERVAL_SECONDS = 0.5
HTTP_TIMEOUT_SECONDS = 10


async def submit_request(client, number, semaphore):
    async with semaphore:
        started = time.perf_counter()
        try:
            response = await client.post(
                f"{BASE_URL}/generate",
                json={"prompt": f"Benchmark request {number}: Explain autoscaling."},
                timeout=HTTP_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            body = response.json()
            request_id = body.get("request_id")
            if not request_id:
                raise ValueError("API response did not include request_id")
            return {
                "number": number,
                "request_id": request_id,
                "started": started,
                "accepted": True,
                "error": None,
            }
        except Exception as exc:
            return {
                "number": number,
                "request_id": None,
                "started": started,
                "accepted": False,
                "error": str(exc),
            }


def optional_float(value):
    try:
        return None if value is None or value == "" else float(value)
    except (TypeError, ValueError):
        return None


async def wait_for_result(client, item):
    deadline = time.perf_counter() + COMPLETION_TIMEOUT_SECONDS
    last_error = None

    while time.perf_counter() < deadline:
        try:
            response = await client.get(
                f"{BASE_URL}/result/{item['request_id']}",
                timeout=HTTP_TIMEOUT_SECONDS,
            )
            if response.status_code == 404:
                last_error = "result not found yet"
            else:
                response.raise_for_status()
                body = response.json()
                status = str(body.get("status", "")).lower()

                if status in {"completed", "failed", "error"}:
                    finished = time.perf_counter()
                    return {
                        **item,
                        "status": "completed" if status == "completed" else "failed",
                        "end_to_end": finished - item["started"],
                        "queue_wait": optional_float(body.get("queue_wait_seconds")),
                        "inference": optional_float(body.get("inference_seconds")),
                        "error": body.get("error") or body.get("detail"),
                    }
                last_error = None
        except Exception as exc:
            last_error = str(exc)

        await asyncio.sleep(POLL_INTERVAL_SECONDS)

    return {
        **item,
        "status": "timed_out",
        "end_to_end": time.perf_counter() - item["started"],
        "queue_wait": None,
        "inference": None,
        "error": last_error or "completion deadline exceeded",
    }


def report_distribution(label, values):
    if not values:
        print(f"{label}: n/a")
        return
    ordered = sorted(values)
    p95_index = max(0, min(len(ordered) - 1, int(0.95 * len(ordered)) - 1))
    print(
        f"{label}: mean={statistics.mean(ordered):.2f}s, "
        f"median={statistics.median(ordered):.2f}s, "
        f"p95={ordered[p95_index]:.2f}s"
    )


async def main():
    print("=" * 60)
    print("AUTOSCALER BENCHMARK — SIMULATED INFERENCE")
    print(f"Requests: {TOTAL_REQUESTS} | concurrency: {CONCURRENCY}")
    print("=" * 60)
    print("Keep run_autoscaler.py running in another terminal.")

    semaphore = asyncio.Semaphore(CONCURRENCY)
    limits = httpx.Limits(
        max_connections=CONCURRENCY,
        max_keepalive_connections=CONCURRENCY,
    )
    started = time.perf_counter()

    async with httpx.AsyncClient(
        limits=limits, timeout=httpx.Timeout(HTTP_TIMEOUT_SECONDS)
    ) as client:
        try:
            response = await client.get(f"{BASE_URL}/docs")
            response.raise_for_status()
        except Exception as exc:
            print(f"API is unreachable at {BASE_URL}: {exc}")
            return

        submissions = await asyncio.gather(*[
            submit_request(client, i, semaphore)
            for i in range(1, TOTAL_REQUESTS + 1)
        ])
        accepted = [item for item in submissions if item["accepted"]]
        rejected = [item for item in submissions if not item["accepted"]]

        print(
            f"Accepted: {len(accepted)}/{TOTAL_REQUESTS}; "
            f"submission failures: {len(rejected)}"
        )
        for item in rejected[:5]:
            print(f"Submission {item['number']} failed: {item['error']}")

        results = await asyncio.gather(*[
            wait_for_result(client, item) for item in accepted
        ])

    elapsed = time.perf_counter() - started
    completed = [r for r in results if r["status"] == "completed"]
    failed = [r for r in results if r["status"] == "failed"]
    timed_out = [r for r in results if r["status"] == "timed_out"]

    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)
    print(f"Submitted:             {len(submissions)}")
    print(f"Accepted:              {len(accepted)}")
    print(f"Completed:             {len(completed)}")
    print(f"Failed (including submit failures): {len(failed) + len(rejected)}")
    print(f"Timed out:              {len(timed_out)}")
    print(f"Total elapsed:          {elapsed:.2f}s")
    print(f"Completion throughput:  {len(completed) / elapsed:.2f} requests/s")

    report_distribution(
        "End-to-end latency",
        [r["end_to_end"] for r in completed],
    )
    report_distribution(
        "Queue wait (worker-reported)",
        [r["queue_wait"] for r in completed if r["queue_wait"] is not None],
    )
    report_distribution(
        "Inference duration (worker-reported)",
        [r["inference"] for r in completed if r["inference"] is not None],
    )

    if failed:
        print("\nFailed request examples:")
        for item in failed[:5]:
            print(f"  {item['request_id']}: {item.get('error')}")
    if timed_out:
        print("\nTimed-out request examples:")
        for item in timed_out[:5]:
            print(f"  {item['request_id']}: {item.get('error')}")

    print("\nWait for queue depth and active requests to reach 0.")
    print("Then confirm the controller scales back down to 1 worker.")


if __name__ == "__main__":
    asyncio.run(main())
