from app.providers.base import BaseProvider, ProviderResult
from app.providers.fcm_provider import FCMProvider
from app.providers.whatsapp_provider import WhatsAppProvider
from app.providers.smtp_provider import SMTPProvider
from app.providers.chaos_provider import ChaosSimulationProvider

_PROVIDERS: dict[str, BaseProvider] = {
    "FCM": FCMProvider(),
    "WHATSAPP": WhatsAppProvider(),
    "EMAIL": SMTPProvider(),
    "WEBHOOK": ChaosSimulationProvider(failure_rate=0.0),
}


def get_provider(channel_name: str) -> BaseProvider:
    channel = channel_name.upper()
    if channel in _PROVIDERS:
        return _PROVIDERS[channel]
    return _PROVIDERS["WEBHOOK"]


def register_provider(channel_name: str, provider: BaseProvider) -> None:
    _PROVIDERS[channel_name.upper()] = provider


__all__ = [
    "BaseProvider",
    "ProviderResult",
    "FCMProvider",
    "WhatsAppProvider",
    "SMTPProvider",
    "ChaosSimulationProvider",
    "get_provider",
    "register_provider",
]
