# Heterogeneous LLM Inference & Fuzzy-Logic Autoscaling Control Plane

A cloud-native, workload-aware LLM inference orchestration platform built with **FastAPI**, **Redis**, **Docker Compose**, and **Kubernetes (`k3s`)** on **Oracle Cloud Infrastructure (OCI)**.

Instead of relying on lagging CPU/memory thresholds (which miss network- and GPU-bound inference backlogs) or routing every prompt through a single expensive model, this platform classifies incoming prompts by complexity, isolates traffic across **Fast** and **Heavy** worker pools, and dynamically scales pod replicas using a **fuzzy-logic predictive controller**.

---

## Table of Contents
1. [System Architecture](#system-architecture)
2. [Core Engineering Components](#core-engineering-components)
3. [Repository Structure](#repository-structure)
4. [API Reference](#api-reference)
5. [Benchmark Results & Scaling Telemetry](#benchmark-results--scaling-telemetry)
6. [Deployment Guide](#deployment-guide)
7. [Operations & Demo Checklist](#operations--demo-checklist)

---

## System Architecture

The platform separates request execution (**Data Plane**) from telemetry-driven replica management (**Control Plane**):

```text
                              DATA PLANE
  Client / Load Generator
            |
            v
   FastAPI Gateway (:8000 / NodePort :30080)
   +-- Complexity Router (prompt length > 120, max_tokens > 150, or explicit tier)
            |
     +------+------+
     |             |
     v             v
Redis Queue:   Redis Queue:
   fast           heavy
     |             |
     v             v
Worker Pool:   Worker Pool:
Fast (1..4)    Heavy (1..2)
     |             |
     +------+------+
            |
            v
   LLM Inference Engine
   (Local Ollama / Groq Cloud API + 500-req Quota Cap)
            |
            v
   Redis Result Store (/result/{request_id})


                            CONTROL PLANE
  Redis Telemetry (Queue Depth, Growth Velocity, Rolling Latency, Active Leases)
            |
            v
   Fuzzy-Logic Autoscaler (3s polling loop, 15s cooldown, 3-sample idle check)
            |
     +------+------+
     |             |
     v             v
Docker Compose   Kubernetes API
(--scale CLI)    (deployments/scale RBAC)
```

---

## Core Engineering Components

### 1. API Gateway & Complexity Router (`app/routes/inference.py`)
- Exposes asynchronous REST endpoints (`/generate`, `/result/{id}`, `/queue/status`, `/workers`, `/autoscaler/*`) so HTTP clients never block on multi-second LLM generation.
- **Tier Classification (`select_tier`):** Routes lightweight queries (`len(prompt) <= 120` and `max_tokens <= 150`) to `inference_queue:fast`, while steering long-context or high-token reasoning requests (`len(prompt) > 120`, `max_tokens > 150`, or `"tier": "heavy"`) to `inference_queue:heavy`.

### 2. Redis Broker, Heartbeats & Lease Recovery (`redis_service.py`, `worker_registry.py`)
- **Isolated Queues:** Decouples fast and heavy workloads into independent Redis lists so heavy generations never cause head-of-line blocking for short queries.
- **Worker Registry & Scale-Down Safety:** Workers publish periodic heartbeats, active request leases, throughput, and rolling latency metrics in Redis hashes. If a pod terminates mid-flight during scale-down, uncompleted leases are recovered cleanly without dropping requests.

### 3. Heterogeneous Worker Pools (`app/worker/worker.py`, `app/config.py`)
- **Dual Inference Drivers:** Supports local execution via **Ollama** (`qwen2.5:0.5b` / `llama3.2:1b` for fast; `qwen2.5:3b` for heavy) and hybrid cloud execution via **Groq** (`openai/gpt-oss-20b` / `llama-3.1-8b-instant` for fast; `openai/gpt-oss-120b` / `llama-3.3-70b-versatile` for heavy).
- **Quota Guardrail:** Tracks `cloud:requests_used` atomically in Redis and halts external API calls once `CLOUD_MAX_REQUESTS` (default `500`) is reached.

### 4. Fuzzy-Logic Predictive Autoscaler (`app/services/autoscaler.py`, `run_autoscaler.py`)
- Evaluates three normalized signals per tier every **3 seconds**:
  1. **Queue Depth:** Current pending backlog in Redis.
  2. **Queue Growth Rate:** Arrival velocity (requests/minute) to detect bursts before latency degrades.
  3. **SLO-Normalized Latency:** Rolling inference latency compared against tier targets (`2.5s` fast / `12.0s` heavy) with idle decay.
- **Anti-Thrashing Controls:** Enforces a **15-second cooldown** between scaling actions and requires **3 consecutive idle observations** (`Queue=0`, `Active=0`) before scaling down.
- **Dual Actuation:** Runs locally with `--backend docker` (`docker compose up -d --scale`) or in-cluster with `--backend k8s` (Kubernetes RBAC `deployments/scale`).

---

## Repository Structure

```text
Autoscaler/
|-- README.md
|-- .gitignore
|-- .env.example
`-- fastapi-service/
    |-- .env.example              # Environment configuration schema (secrets excluded)
    |-- Dockerfile                # Multi-arch image bundling Python app + kubectl CLI
    |-- compose.yaml              # Local Docker Compose stack (Redis, API, Fast/Heavy Workers)
    |-- run_autoscaler.py         # Dual-backend autoscaler daemon (--backend docker | k8s)
    |-- benchmark_autoscaler.py   # Async concurrent load generator & latency profiler
    |-- app/
    |   |-- config.py             # Auto-loads .env and typed Pydantic settings
    |   |-- main.py               # FastAPI lifecycle & router registration
    |   |-- models/schemas.py     # Request/response validation models
    |   |-- routes/inference.py   # Complexity router, polling & autoscaler telemetry routes
    |   |-- services/
    |   |   |-- redis_service.py
    |   |   |-- worker_registry.py
    |   |   `-- autoscaler.py     # Fuzzy membership functions & scaling policy
    |   `-- worker/
    |       `-- worker.py         # Tier-aware async queue consumer (Ollama / Groq)
    `-- k8s/
        |-- 00-namespace.yaml     # llm-autoscaler namespace
        |-- 01-redis.yaml         # Redis Deployment + ClusterIP Service
        |-- 02-api.yaml           # FastAPI Deployment + NodePort Service (:30080)
        |-- 03-workers.yaml       # worker-fast (1..4) & worker-heavy (1..2) Deployments
        `-- 04-autoscaler.yaml    # ServiceAccount, Role, RoleBinding & Fuzzy Autoscaler Pod
```

---

## API Reference

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `POST` | `/generate` | Enqueues a prompt (`prompt`, `max_tokens`, optional `tier`) and returns `request_id` + assigned tier. |
| `GET` | `/result/{request_id}` | Polls job status (`queued`, `processing`, `completed`, `failed`), output text, and wait/inference latency. |
| `GET` | `/queue/status` | Returns real-time queue depths for `inference_queue:fast` and `inference_queue:heavy`. |
| `GET` | `/workers` | Lists all registered workers, tiers, heartbeats, active leases, and rolling latency stats. |
| `GET` | `/autoscaler/status` | Returns current fuzzy scores, replica counts, cooldown state, and idle streaks per tier. |
| `POST` | `/autoscaler/evaluate` | Triggers an immediate fuzzy evaluation cycle and returns scaling recommendations. |
| `GET` | `/health` | Liveness/readiness check verifying API and Redis connectivity. |

---

## Benchmark Results & Scaling Telemetry

Evaluated on an **Oracle Cloud Infrastructure (OCI)** `k3s` cluster (`NodePort :30080`) backed by Groq cloud inference (`openai/gpt-oss-20b` on `fast`, `openai/gpt-oss-120b` on `heavy`):

### 1. Measured Heterogeneous Workload Runs
| Run Scenario | Requests (Concurrency) | Completion | Wall Time | Fast Tier Metrics (`gpt-oss-20b`) | Heavy Tier Metrics (`gpt-oss-120b`) | Observed Scaling Behavior |
| :--- | :---: | :---: | :---: | :--- | :--- | :--- |
| **Mixed Heterogeneous Burst** | 24 (12) | **24/24 (100%)** | **7.14s** | `n=18`, avg wait `2.29s`, inf `0.38s`, P95 `3.79s` | `n=6`, avg wait `0.70s`, inf `0.63s`, P95 `2.28s` | Clean complexity split across isolated queues with zero failures |
| **Heavy-Tier Saturation Burst** | 36 (24) | 29/36 (7 hit 30 RPM cap) | 16.44s | Idle (`1` replica, `score=0.40`) | `n=29`, avg wait `7.93s`, inf `0.90s`, P95 `13.39s` | Live `worker-heavy` scale-up (`1 -> 2` pods) under backlog spike |
| **Full Scale-Up / Down Lifecycle** | 48 (24) | **48/48 (100%)** | ~11.2s | Scaled `1 -> 2` pods, drained to `Queue=0`, scaled `2 -> 1` | Held at `1` replica | Verified zero-drop scale-down after cooldown + idle streak |

### 2. Provisioning Strategy Comparison (`36 Requests`, `Concurrency=24`, `140` Fast / `350` Heavy Tokens)
| Provisioning Strategy | Fast / Heavy Replicas | Avg Latency (s) | P95 Latency (s) | Total Worker-Seconds | Infrastructure Cost vs. Fixed-Max |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **Fixed-1 Baseline** | `1` / `1` (Static) | `5.59s` | `7.66s` | `36.9 pod-s` | Under-provisioned (queue backlog under bursts) |
| **Fixed-Max Baseline** | `4` / `2` (Static) | `3.11s` | `4.53s` | `110.8 pod-s` | `0%` (pays for 6 pods continuously) |
| **Fuzzy Autoscaler** | `1..4` / `1..2` (Dynamic) | `7.75s` | `12.59s` | `36.9 pod-s` | **`66.7%` compute reduction** vs. Fixed-Max |

---

## Deployment Guide

### Option A: Local Deployment (Docker Compose)
```bash
cd fastapi-service
cp .env.example .env
docker compose up -d --build
python3 run_autoscaler.py --backend docker
```

### Option B: Cloud Deployment (Kubernetes / `k3s`)
```bash
cd fastapi-service
kubectl apply -f k8s/00-namespace.yaml
kubectl create secret generic llm-cloud-secrets --from-env-file=.env -n llm-autoscaler --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f k8s/
kubectl get pods -n llm-autoscaler -w
```

---

## Operations & Demo Checklist

1. **Reset the 500-Request Cloud Safety Counter Before Demo:**
   ```bash
   kubectl -n llm-autoscaler exec deploy/redis -- redis-cli set cloud:requests_used 0
   ```
2. **Stream Live Scaling Decisions:**
   ```bash
   kubectl -n llm-autoscaler logs -f deploy/fuzzy-autoscaler
   ```
3. **Run the Concurrent Load Benchmark:**
   ```bash
   cd fastapi-service && API_URL="[http://130.210.7.109:30080](http://130.210.7.109:30080)" TOTAL_REQUESTS=36 CONCURRENCY=24 python3 benchmark_autoscaler.py
   ```
4. **Park Cluster When Idle (Scale to Zero Replicas):**
   ```bash
   kubectl -n llm-autoscaler scale deploy/redis deploy/api deploy/worker-fast deploy/worker-heavy deploy/fuzzy-autoscaler --replicas=0
   ```