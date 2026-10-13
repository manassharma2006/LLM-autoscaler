
from app.services.fuzzy_scaler import fuzzy_scaler

scenarios = [
    {
        "name": "Idle",
        "queue_depth": 0,
        "queue_growth_per_minute": -2,
        "latency_seconds": 0.5,
    },
    {
        "name": "Moderate load",
        "queue_depth": 12,
        "queue_growth_per_minute": 10,
        "latency_seconds": 3.5,
    },
    {
        "name": "Traffic burst",
        "queue_depth": 40,
        "queue_growth_per_minute": 30,
        "latency_seconds": 7.0,
    },
]

for scenario in scenarios:
    name = scenario["name"]
    inputs = {k: v for k, v in scenario.items() if k != "name"}
    result = fuzzy_scaler.evaluate(**inputs)

    print(f"\nScenario: {name}")
    print(f"  Score:  {result.score}")
    print(f"  Action: {result.action}")
