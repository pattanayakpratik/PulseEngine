import asyncio
import random
import uuid
from typing import Any, Dict
from app.providers.base import BaseProvider, ProviderResult


class FCMProvider(BaseProvider):
    """Firebase Cloud Messaging (FCM) Adapter."""

    def __init__(self, simulate_latency: bool = True):
        self.simulate_latency = simulate_latency

    async def send(self, payload: Dict[str, Any]) -> ProviderResult:
        if self.simulate_latency:
            # High-performance p99 ~ 25-50ms simulation
            await asyncio.sleep(random.uniform(0.02, 0.05))

        fcm_token = payload.get("user_fcm")
        if not fcm_token:
            return ProviderResult(
                success=False,
                error="Recipient has no registered FCM device token",
            )

        message_id = f"projects/pulse-engine/messages/{uuid.uuid4().hex}"
        return ProviderResult(
            success=True,
            provider_message_id=message_id,
            response_data={"multicast_id": random.randint(100000, 999999), "canonical_ids": 0},
        )
