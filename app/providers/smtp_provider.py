import asyncio
import random
import uuid
from typing import Any, Dict
from app.providers.base import BaseProvider, ProviderResult


class SMTPProvider(BaseProvider):
    """Transactional Email Provider (SMTP / Amazon SES)."""

    def __init__(self, simulate_latency: bool = True):
        self.simulate_latency = simulate_latency

    async def send(self, payload: Dict[str, Any]) -> ProviderResult:
        if self.simulate_latency:
            # SES roundtrip ~ 50-120ms
            await asyncio.sleep(random.uniform(0.05, 0.12))

        email = payload.get("user_email")
        if not email:
            return ProviderResult(
                success=False,
                error="Recipient email address missing for EMAIL channel",
            )

        message_id = f"<{uuid.uuid4().hex}@email.pulseengine.io>"
        return ProviderResult(
            success=True,
            provider_message_id=message_id,
            response_data={"status": "delivered", "provider": "SES"},
        )
