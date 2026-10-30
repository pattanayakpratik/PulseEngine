import asyncio
import random
import uuid
from typing import Any, Dict
from app.providers.base import BaseProvider, ProviderResult


class WhatsAppProvider(BaseProvider):
    """WhatsApp Cloud API Adapter (Meta Graph API)."""

    def __init__(self, simulate_latency: bool = True):
        self.simulate_latency = simulate_latency

    async def send(self, payload: Dict[str, Any]) -> ProviderResult:
        if self.simulate_latency:
            # WhatsApp typical API round-trip ~ 40-90ms
            await asyncio.sleep(random.uniform(0.04, 0.09))

        phone = payload.get("user_phone")
        if not phone:
            return ProviderResult(
                success=False,
                error="Recipient phone number missing for WhatsApp channel",
            )

        wamid = f"wamid.HBgL{uuid.uuid4().hex}"
        return ProviderResult(
            success=True,
            provider_message_id=wamid,
            response_data={"messaging_product": "whatsapp", "status": "accepted"},
        )
