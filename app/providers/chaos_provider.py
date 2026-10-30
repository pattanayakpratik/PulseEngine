import asyncio
import random
from typing import Any, Dict
from app.providers.base import BaseProvider, ProviderResult


class ChaosSimulationProvider(BaseProvider):
    """
    Chaos Engineering & Outage Simulator Provider.
    Allows injecting artificial latency, controlled error rates, and simulating
    provider outages to demonstrate Circuit Breaker tripping and DLQ recovery.
    """

    def __init__(self, failure_rate: float = 0.0, base_latency_ms: float = 40.0, simulated_error: str | None = None):
        self.failure_rate = failure_rate
        self.base_latency_ms = base_latency_ms
        self.simulated_error = simulated_error

    async def send(self, payload: Dict[str, Any]) -> ProviderResult:
        await asyncio.sleep(self.base_latency_ms / 1000.0)

        # Force failure if configured or if probability matches
        if self.simulated_error or random.random() < self.failure_rate:
            error_msg = self.simulated_error or "503 Service Unavailable: Upstream provider gateway timeout"
            return ProviderResult(
                success=False,
                error=error_msg,
                response_data={"http_status": 503, "error_type": "PROVIDER_OUTAGE"},
            )

        return ProviderResult(
            success=True,
            provider_message_id=f"chaos_sim_{random.randint(100000, 999999)}",
            response_data={"status": "simulated_ok"},
        )
