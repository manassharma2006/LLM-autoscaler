
import asyncio
import time

from app.config import settings
from app.services.fuzzy_scaler import fuzzy_scaler
from app.services.redis_service import redis_service
from app.services.worker_registry import worker_registry

class AutoscalerService:
    def __init__(self):
        self.min_replicas = 1
        self.max_replicas = 5
        self.current_replicas = 1
        self.cooldown_seconds = 15
        self.last_scaling_action_at = 0.0
        self.previous_queue_depth = None
        self.previous_sample_time = None
        self.recommended_replicas = self.min_replicas
        self.last_decision = None
        self.last_sample = None
        self.low_load_streak = 0
        self.scale_down_samples_required = 3
        # Demo defaults; calibrate using measured worker capacity.
        self.queue_growth_limit = 60.0
        self._evaluation_lock = asyncio.Lock()
    async def sync_replica_state(self):
        workers = await worker_registry.list_workers()

        healthy_count = sum(
            1 for worker in workers
            if worker["status"] == "healthy"
        )

        self.current_replicas = healthy_count

        # Initialize the recommendation from reality on first sync.
        if self.last_decision is None:
            self.recommended_replicas = max(
                self.min_replicas,
                min(self.max_replicas, healthy_count),
            )

        return healthy_count
    async def collect_metrics(self):
        workers = await worker_registry.list_workers()

        worker_ids = [
            worker["worker_id"]
            for worker in workers
            if worker["status"] == "healthy"
        ]

        queue_depth = await redis_service.get_queue_depth()

        active_requests = sum(
            int(worker["active_requests"])
            for worker in workers
            if worker["status"] == "healthy"
        )

        latencies = [
            float(worker["average_inference_seconds"])
            for worker in workers
            if worker["status"] == "healthy"
            and worker["average_inference_seconds"] is not None
        ]

        observed_latency = (
            sum(latencies) / len(latencies) if latencies else 0.5
        )

        now = time.monotonic()

        if self.previous_queue_depth is None:
            growth_per_minute = 0.0
        else:
            elapsed = max(now - self.previous_sample_time, 0.1)
            growth_per_minute = (
                (queue_depth - self.previous_queue_depth)
                / elapsed
                * 60
            )

        self.previous_queue_depth = queue_depth
        self.previous_sample_time = now

        return {
            "queue_depth": queue_depth,
            "queue_growth_per_minute": round(growth_per_minute, 2),
            "active_requests": active_requests,
            "healthy_workers": len(worker_ids),
            "observed_latency_seconds": round(observed_latency, 3),
        }

    async def evaluate(self):
        async with self._evaluation_lock:
            return await self._evaluate_locked()

    async def _evaluate_locked(self):
        await self.sync_replica_state()
        metrics = await self.collect_metrics()
        decision = fuzzy_scaler.evaluate(
            queue_depth=metrics["queue_depth"],
            queue_growth_per_minute=metrics["queue_growth_per_minute"],
            latency_seconds=metrics["observed_latency_seconds"],
        )

        # Demo policy: translate fuzzy urgency into a replica recommendation.
        
        now = time.monotonic()
        cooldown_active = (
            now - self.last_scaling_action_at < self.cooldown_seconds
        )

        # Start from the last persistent recommendation.
        desired = self.recommended_replicas

        queue_depth = metrics["queue_depth"]
        active_requests = metrics["active_requests"]

        # Scale down only after sustained idle conditions.
        safely_idle = (
            queue_depth == 0
            and active_requests == 0
        )

        if safely_idle:
            self.low_load_streak += 1
        else:
            self.low_load_streak = 0

        if not cooldown_active:
            if decision.action in (
                "scale_up",
                "scale_up_aggressively",
            ):
                # Increase by one normally; increase by two only
                # for a severe backlog.
                increment = (
                    2
                    if decision.action == "scale_up_aggressively"
                    and queue_depth >= 25
                    else 1
                )
                desired += increment

            elif (
                not cooldown_active
                and self.low_load_streak >= self.scale_down_samples_required
                and safely_idle
            ):
                desired -= 1
            desired = max(
                self.min_replicas,
                min(self.max_replicas, desired),
            )

            if desired != self.recommended_replicas:
                self.recommended_replicas = desired
                self.last_scaling_action_at = now

                # Reset the idle streak after a scaling recommendation.
                self.low_load_streak = 0

        self.last_sample = metrics
        self.last_decision = {
            "score": decision.score,
            "action": decision.action,
            "current_replicas": self.current_replicas,
            "recommended_replicas": self.recommended_replicas,
            "mode": "decision_only",
            "cooldown_active": cooldown_active,
            "low_load_streak": self.low_load_streak,
        }

        return {
            "metrics": metrics,
            "decision": self.last_decision,
        }



    def status(self):
        return {
            "mode": "decision_only",
            "last_sample": self.last_sample,
            "last_decision": self.last_decision,
            "replica_limits": {
                "minimum": self.min_replicas,
                "maximum": self.max_replicas,
            },
        }


autoscaler_service = AutoscalerService()
