import os
from app.config import settings  # loads .env
import argparse
import json
import os
import subprocess
import time
import urllib.request

API_URL = os.getenv("AUTOSCALER_API_URL", "http://127.0.0.1:8000/autoscaler/evaluate")
K8S_NAMESPACE = os.getenv("K8S_NAMESPACE", "llm-autoscaler")
TIER_LIMITS = {"fast": (1, 4), "heavy": (1, 2)}
POLL_INTERVAL_SECONDS = float(os.getenv("AUTOSCALER_POLL_SECONDS", "3"))
SCALE_RETRIES = 3


def evaluate_autoscaler(api_url: str):
    req = urllib.request.Request(
        api_url, data=b"", method="POST", headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode())


# --- Docker Compose Backend ---
def docker_get_running(service_name: str) -> int:
    res = subprocess.run(
        ["docker", "compose", "ps", "--format", "json", service_name],
        capture_output=True, text=True, check=False,
    )
    if res.returncode or not res.stdout.strip():
        return 0
    out = res.stdout.strip()
    try:
        records = json.loads(out)
        records = records if isinstance(records, list) else [records]
    except json.JSONDecodeError:
        records = [json.loads(line) for line in out.splitlines() if line.strip()]
    return sum(str(item.get("State", "")).lower() == "running" for item in records)


def docker_scale(service_name: str, replicas: int) -> bool:
    for attempt in range(1, SCALE_RETRIES + 1):
        print(f"[docker:{service_name}] Scaling to {replicas} (attempt {attempt}/{SCALE_RETRIES})")
        res = subprocess.run(
            ["docker", "compose", "up", "-d", "--scale", f"{service_name}={replicas}"],
            capture_output=True, text=True, check=False,
        )
        if not res.returncode:
            for _ in range(5):
                if docker_get_running(service_name) == replicas:
                    return True
                time.sleep(1)
        time.sleep(attempt)
    return False


# --- Kubernetes (k3s / EKS / GKE) Backend ---
def k8s_get_running(deployment_name: str, namespace: str) -> int:
    res = subprocess.run(
        ["kubectl", "-n", namespace, "get", "deployment", deployment_name, "-o", "json"],
        capture_output=True, text=True, check=False,
    )
    if res.returncode or not res.stdout.strip():
        raise RuntimeError(res.stderr.strip() or f"kubectl get deployment {deployment_name} failed")
    data = json.loads(res.stdout)
    return int(data.get("status", {}).get("readyReplicas") or data.get("spec", {}).get("replicas") or 0)


def k8s_scale(deployment_name: str, replicas: int, namespace: str) -> bool:
    for attempt in range(1, SCALE_RETRIES + 1):
        print(f"[k8s:{namespace}/{deployment_name}] Scaling pods to {replicas} (attempt {attempt}/{SCALE_RETRIES})")
        res = subprocess.run(
            ["kubectl", "-n", namespace, "scale", f"deployment/{deployment_name}", f"--replicas={replicas}"],
            capture_output=True, text=True, check=False,
        )
        if not res.returncode:
            print(f"[k8s:{namespace}/{deployment_name}] Kubernetes API accepted replicas={replicas}")
            return True
        print(f"[k8s] Scale error: {res.stderr.strip()}")
        time.sleep(attempt)
    return False


def should_scale_down(metrics, desired, actual):
    return desired < actual and metrics["queue_depth"] == 0 and metrics["active_requests"] == 0


def main():
    parser = argparse.ArgumentParser(description="Heterogeneous LLM Fuzzy Autoscaler")
    parser.add_argument("--backend", choices=["docker", "k8s"], default=os.getenv("AUTOSCALER_BACKEND", "k8s"))
    parser.add_argument("--api-url", default=API_URL)
    parser.add_argument("--namespace", default=K8S_NAMESPACE)
    args = parser.parse_args()

    print(f"Heterogeneous autoscaler started | backend={args.backend} | api={args.api_url} | poll={POLL_INTERVAL_SECONDS}s")
    while True:
        try:
            tiers = evaluate_autoscaler(args.api_url)["tiers"]
            for tier, (min_r, max_r) in TIER_LIMITS.items():
                target_name = f"worker-{tier}"
                decision = tiers[tier]["decision"]
                metrics = tiers[tier]["metrics"]
                desired = max(min_r, min(max_r, int(decision["recommended_replicas"])))

                actual = (
                    k8s_get_running(target_name, args.namespace)
                    if args.backend == "k8s"
                    else docker_get_running(target_name)
                )

                print(
                    f"[{tier.upper():5s}] Queue={metrics['queue_depth']} | "
                    f"Active={metrics['active_requests']} | Healthy={metrics['healthy_workers']} | "
                    f"Ready={actual} | Desired={desired} | Action={decision['action']}"
                )

                if desired > actual or should_scale_down(metrics, desired, actual):
                    ok = (
                        k8s_scale(target_name, desired, args.namespace)
                        if args.backend == "k8s"
                        else docker_scale(target_name, desired)
                    )
                    if not ok:
                        print(f"[{target_name}] Scale failed after retries")
        except Exception as exc:
            print(f"Autoscaler loop error: {exc}")
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
