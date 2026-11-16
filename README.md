# Heterogeneous LLM Inference & Fuzzy-Logic Autoscaling

Workload-aware LLM inference orchestration built with **FastAPI**, **Redis**, **Docker Compose**, and **Kubernetes (k3s)**.

## Architecture
- **Complexity Router (`fastapi-service/app/routes/inference.py`):** Routes lightweight queries to `fast` and complex prompts (`len > 120` or `max_tokens > 150`) to `heavy`.
- **Redis Broker & Worker Registry:** Manages isolated per-tier queues, worker heartbeats, request leases, and rolling latencies.
- **Fuzzy-Logic Autoscaler (`app/services/autoscaler.py`):** Scales `fast` and `heavy` worker replicas via Docker Compose (`--backend docker`) or Kubernetes (`--backend k8s`).

## Quick Start
1. Copy `fastapi-service/.env.example` to `fastapi-service/.env` and set your backend/API key.
2. **Docker Compose:** `docker compose up -d --build` and `python3 run_autoscaler.py --backend docker`
3. **Kubernetes (k3s):** Create secret `llm-cloud-secrets` from `.env`, then `kubectl apply -f k8s/`
