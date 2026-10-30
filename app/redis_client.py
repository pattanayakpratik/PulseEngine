import json
from typing import Optional, Any
from redis.asyncio import Redis, from_url
from app.config import settings

_redis_instance: Optional[Redis] = None


def get_redis_client() -> Redis:
    global _redis_instance
    if _redis_instance is None:
        _redis_instance = from_url(
            settings.REDIS_URL,
            encoding="utf-8",
            decode_responses=True,
        )
    return _redis_instance


async def close_redis_client() -> None:
    global _redis_instance
    if _redis_instance is not None:
        await _redis_instance.close()
        _redis_instance = None


class IdempotencyManager:
    """Manages atomic request locks and cached responses via Redis."""

    def __init__(self, redis: Redis):
        self.redis = redis

    @staticmethod
    def _key(idempotency_key: str) -> str:
        return f"idemp:{idempotency_key}"

    async def acquire_lock(self, idempotency_key: str, ttl_seconds: int = 86400) -> tuple[bool, Optional[dict]]:
        """
        Attempts to acquire an idempotency lock for the given key.
        Returns:
            (True, None) -> Lock acquired successfully, caller should proceed.
            (False, None) -> Lock currently in flight ('PROCESSING'), return 409 Conflict.
            (False, cached_data) -> Request already completed, return cached response (200 OK).
        """
        key = self._key(idempotency_key)
        # Attempt atomic set if not exists
        acquired = await self.redis.set(key, "PROCESSING", nx=True, ex=ttl_seconds)
        if acquired:
            return True, None

        # Key already exists: check if processing or completed with payload
        current_val = await self.redis.get(key)
        if current_val == "PROCESSING":
            return False, None
        
        try:
            cached_data = json.loads(current_val) if current_val else None
            return False, cached_data
        except (json.JSONDecodeError, TypeError):
            return False, None

    async def commit_response(self, idempotency_key: str, response_payload: dict, ttl_seconds: int = 86400) -> None:
        """Stores the final committed HTTP response payload in Redis under the idempotency key."""
        key = self._key(idempotency_key)
        await self.redis.set(key, json.dumps(response_payload), ex=ttl_seconds)

    async def release_lock(self, idempotency_key: str) -> None:
        """Releases the lock on failure so the client may safely retry."""
        key = self._key(idempotency_key)
        val = await self.redis.get(key)
        if val == "PROCESSING":
            await self.redis.delete(key)
