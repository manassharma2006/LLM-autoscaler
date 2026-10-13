
from dataclasses import dataclass


def clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def triangle(value: float, left: float, peak: float, right: float) -> float:
    """Triangular fuzzy membership, returning a value between 0 and 1."""
    if value < left or value > right:
        return 0.0

    if value == peak:
        return 1.0

    if value < peak:
        return (value - left) / (peak - left) if peak != left else 1.0

    return (right - value) / (right - peak) if right != peak else 1.0


def low_membership(value: float, full_until: float, zero_at: float) -> float:
    if value <= full_until:
        return 1.0
    if value >= zero_at:
        return 0.0
    return (zero_at - value) / (zero_at - full_until)


def high_membership(value: float, zero_until: float, full_at: float) -> float:
    if value <= zero_until:
        return 0.0
    if value >= full_at:
        return 1.0
    return (value - zero_until) / (full_at - zero_until)


@dataclass
class ScalingDecision:
    score: float
    action: str
    queue_pressure: float
    growth_pressure: float
    latency_pressure: float


class FuzzyScaler:
    """
    Rule-based fuzzy controller.

    Inputs:
      queue_depth: number of requests waiting in Redis
      queue_growth_per_minute: change in queue depth per minute
      latency_seconds: observed average inference latency

    Output:
      score between 0 and 1, interpreted as scaling urgency.
    """

    def evaluate(
        self,
        queue_depth: int,
        queue_growth_per_minute: float,
        latency_seconds: float,
    ) -> ScalingDecision:
        # Normalize raw measurements into fuzzy memberships.
        q_low = low_membership(max(queue_depth, 0), 1, 10)
        q_medium = triangle(max(queue_depth, 0), 2, 10, 20)
        q_high = high_membership(max(queue_depth, 0), 10, 25)

        growth = queue_growth_per_minute
        g_low = low_membership(growth, -5, 0)
        g_medium = triangle(growth, -2, 5, 15)
        g_high = high_membership(growth, 5, 20)

        latency = max(latency_seconds, 0.0)
        l_low = low_membership(latency, 1.0, 2.0)
        l_medium = triangle(latency, 1.0, 3.0, 5.0)
        l_high = high_membership(latency, 3.0, 6.0)

        # Fuzzy rules. Each rule has a strength and an output score.
        # 0.0 = no urgency; 1.0 = maximum scaling urgency.
        
        rules = [
            # Severe backlog and high latency.
            (min(q_high, l_high), 0.95),

            # Severe backlog that is still growing.
            (min(q_high, g_high), 0.95),

            # A large backlog alone must create scaling pressure.
            (q_high, 0.85),

            # Medium backlog, rising queue, and elevated latency.
            (min(q_medium, g_high, l_medium), 0.80),

            # High latency even before the queue becomes large.
            (l_high, 0.75),

            # Medium backlog and rising demand.
            (min(q_medium, g_medium), 0.65),

            # Moderate backlog or elevated latency.
            (max(q_medium, l_medium), 0.50),

            # Low and stable workload.
            (min(q_low, g_low, l_low), 0.10),
        ]


        total_strength = sum(strength for strength, _ in rules)

        if total_strength == 0:
            score = 0.40
        else:
            score = sum(
                strength * output_score
                for strength, output_score in rules
            ) / total_strength

        score = clamp(score)

        if score >= 0.80:
            action = "scale_up_aggressively"
        elif score >= 0.60:
            action = "scale_up"
        elif score <= 0.20:
            action = "consider_scale_down"
        else:
            action = "hold"

        return ScalingDecision(
            score=round(score, 3),
            action=action,
            queue_pressure=round(max(q_low * 0, q_medium, q_high), 3),
            growth_pressure=round(max(g_low * 0, g_medium, g_high), 3),
            latency_pressure=round(max(l_low * 0, l_medium, l_high), 3),
        )


fuzzy_scaler = FuzzyScaler()
