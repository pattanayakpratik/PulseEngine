from abc import ABC, abstractmethod
from typing import Any, Dict


class ProviderResult:
    def __init__(self, success: bool, provider_message_id: str | None = None, response_data: dict | None = None, error: str | None = None):
        self.success = success
        self.provider_message_id = provider_message_id
        self.response_data = response_data or {}
        self.error = error

    def to_dict(self) -> dict:
        return {
            "success": self.success,
            "provider_message_id": self.provider_message_id,
            "response_data": self.response_data,
            "error": self.error,
        }


class BaseProvider(ABC):
    """Abstract interface for all notification dispatch providers."""

    @abstractmethod
    async def send(self, payload: Dict[str, Any]) -> ProviderResult:
        """Dispatches notification to third-party upstream API."""
        pass
