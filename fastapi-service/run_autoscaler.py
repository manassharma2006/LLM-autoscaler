import json
import os
import subprocess
import time
import urllib.request

API_URL = os.getenv("AUTOSCALER_API_URL", "http://127.0.0.1:8000/autoscaler/evaluate")
MIN_REPLICAS = 1
MAX_REPLICAS = 5
POLL_INTERVAL_SECONDS = float(os.getenv("AUTOSCALER_POLL_SECONDS", "3"))
SCALE_RETRIES = 3


def evaluate_autoscaler():
    request = urllib.request.Request(
        API_URL, data=b"", method="POST", headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.loads(response.read().decode())


def get_running_workers():
    result = subprocess.run(
        ["docker", "compose", "ps", "--format", "json", "worker"],
        capture_output=True, text=True, check=False,
    )
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or "docker compose ps failed")
    output = result.stdout.strip()
    if not output:
        return 0
    try:
        records = json.loads(output)
        records = records if isinstance(records, list) else [records]
    except json.JSONDecodeError:
        records = []
        for line in output.splitlines():
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return sum(str(item.get("State", "")).lower() == "running" for item in records)


def scale_workers(replicas):
    if not MIN_REPLICAS <= replicas <= MAX_REPLICAS:
        raise ValueError(f"Replica count out of range: {replicas}")
    last_error = None
    for attempt in range(1, SCALE_RETRIES + 1):
        print(f"Scaling worker replicas to {replicas} (attempt {attempt}/{SCALE_RETRIES})")
        result = subprocess.run(
            ["docker", "compose", "up", "-d", "--scale", f"worker={replicas}"],
            capture_output=True, text=True, check=False,
        )
        if result.returncode:
            last_error = result.stderr.strip() or result.stdout.strip()
        else:
            for _ in range(5):
                if get_running_workers() == replicas:
                    print(f"Scale confirmed: running workers={replicas}")
                    return True
                time.sleep(1)
            last_error = "Compose completed but replica count did not converge"
        print(f"Scale attempt failed: {last_error}")
        time.sleep(attempt)
    print(f"Scale to {replicas} not confirmed; will retry next poll: {last_error}")
    return False


def should_scale_down(metrics, desired, actual):
    return desired < actual and metrics["queue_depth"] == 0 and metrics["active_requests"] == 0


def main():
    print(f"Host autoscaling controller started; poll={POLL_INTERVAL_SECONDS}s.")
    print("Press Ctrl+C to stop the controller.")
    while True:
        try:
            result = evaluate_autoscaler()
            decision, metrics = result["decision"], result["metrics"]
            desired = max(MIN_REPLICAS, min(MAX_REPLICAS, int(decision["recommended_replicas"])))
            actual = get_running_workers()
            print(
                f"Queue={metrics['queue_depth']} | Active={metrics['active_requests']} | "
                f"Healthy={metrics['healthy_workers']} | Running={actual} | "
                f"Desired={desired} | Action={decision['action']}"
            )
            if actual != desired:
                if desired < actual and not should_scale_down(metrics, desired, actual):
                    print("Scale-down postponed: queued or active requests remain.")
                else:
                    scale_workers(desired)
        except Exception as exc:
            print(f"Controller error (will retry): {exc}")
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nAutoscaling controller stopped.")
