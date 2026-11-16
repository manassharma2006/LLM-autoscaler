import asyncio
import time
from app.services.fuzzy_scaler import fuzzy_scaler
from app.services.redis_service import redis_service
from app.services.worker_registry import worker_registry


class TierAutoscaler:
    def __init__(self, tier: str, min_replicas: int, max_replicas: int, target_latency: float = 2.0):
        self.tier = tier
        self.min_replicas = min_replicas
        self.max_replicas = max_replicas
        self.target_latency = target_latency
        self.current_replicas = min_replicas
        self.cooldown_seconds = 15
        self.last_scaling_action_at = 0.0
        self.previous_queue_depth = None
        self.previous_sample_time = None
        self.recommended_replicas = min_replicas
        self.last_decision = None
        self.last_sample = None
        self.low_load_streak = 0
        self.scale_down_samples_required = 3

    async def evaluate(self, all_workers: list[dict]):
        tier_workers = [
            w for w in all_workers
            if w["status"] == "healthy" and w.get("tier", "fast") == self.tier
        ]
        healthy_count = len(tier_workers)
        self.current_replicas = healthy_count

        if self.last_decision is None:
            self.recommended_replicas = max(self.min_replicas, min(self.max_replicas, healthy_count or self.min_replicas))

        queue_depth = await redis_service.get_queue_depth(self.tier)
        active_requests = sum(int(w["active_requests"]) for w in tier_workers)
        latencies = [
            float(w["average_inference_seconds"])
            for w in tier_workers
            if w["average_inference_seconds"] is not None
        ]
        raw_latency = sum(latencies) / len(latencies) if latencies else 0.5
        is_idle = (queue_depth == 0 and active_requests == 0)
        effective_latency = 0.5 if is_idle else (raw_latency / self.target_latency) * 2.0
        observed_latency = round(effective_latency, 3)

        now = time.monotonic()
        if self.previous_queue_depth is None:
            growth_per_minute = 0.0
        else:
            elapsed = max(now - self.previous_sample_time, 0.1)
            growth_per_minute = ((queue_depth - self.previous_queue_depth) / elapsed) * 60

        self.previous_queue_depth = queue_depth
        self.previous_sample_time = now

        metrics = {
            "tier": self.tier,
            "queue_depth": queue_depth,
            "queue_growth_per_minute": round(growth_per_minute, 2),
            "active_requests": active_requests,
            "healthy_workers": healthy_count,
            "observed_latency_seconds": round(observed_latency, 3),
        }

        decision = fuzzy_scaler.evaluate(
            queue_depth=queue_depth,
            queue_growth_per_minute=growth_per_minute,
            latency_seconds=observed_latency,
        )

        cooldown_active = (now - self.last_scaling_action_at) < self.cooldown_seconds
        desired = self.recommended_replicas
        safely_idle = queue_depth == 0 and active_requests == 0

        self.low_load_streak = (self.low_load_streak + 1) if safely_idle else 0

        if not cooldown_active:
            if decision.action in ("scale_up", "scale_up_aggressively"):
                increment = 2 if (decision.action == "scale_up_aggressively" and queue_depth >= 15) else 1
                desired += increment
            elif self.low_load_streak >= self.scale_down_samples_required and safely_idle:
                desired -= 1

            desired = max(self.min_replicas, min(self.max_replicas, desired))
            if desired != self.recommended_replicas:
                self.recommended_replicas = desired
                self.last_scaling_action_at = now
                self.low_load_streak = 0

        self.last_sample = metrics
        self.last_decision = {
            "tier": self.tier,
            "score": decision.score,
            "action": decision.action,
            "current_replicas": self.current_replicas,
            "recommended_replicas": self.recommended_replicas,
            "cooldown_active": cooldown_active,
            "low_load_streak": self.low_load_streak,
        }
        return {"metrics": metrics, "decision": self.last_decision}


class AutoscalerService:
    def __init__(self):
        self.tiers = {
            "fast": TierAutoscaler("fast", min_replicas=1, max_replicas=4, target_latency=2.5),
            "heavy": TierAutoscaler("heavy", min_replicas=1, max_replicas=2, target_latency=12.0),
        }
        self._lock = asyncio.Lock()

    async def evaluate(self):
        async with self._lock:
            workers = await worker_registry.list_workers()
            results = {}
            for name, scaler in self.tiers.items():
                results[name] = await scaler.evaluate(workers)
            return {"tiers": results}

    def status(self):
        return {
            "mode": "heterogeneous_decision",
            "tiers": {
                name: {
                    "last_sample": s.last_sample,
                    "last_decision": s.last_decision,
                    "replica_limits": {"minimum": s.min_replicas, "maximum": s.max_replicas},
                }
                for name, s in self.tiers.items()
            },
        }


autoscaler_service = AutoscalerService()
