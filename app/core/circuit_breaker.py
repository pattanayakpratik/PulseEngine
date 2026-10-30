import time
from enum import Enum
from redis.asyncio import Redis
from app.config import settings


class CircuitState(str, Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreakerOpenException(Exception):
    pass


class CircuitBreaker:
    """
    Distributed Redis-backed Circuit Breaker.
    Monitors rolling failure rate per provider and trips to OPEN state to prevent
    cascade failures and hanging sockets when third-party APIs experience outages.
    """

    def __init__(
        self,
        redis: Redis,
        provider_name: str,
        window_seconds: int = settings.CIRCUIT_BREAKER_WINDOW_SECONDS,
        failure_threshold: float = settings.CIRCUIT_BREAKER_FAILURE_THRESHOLD,
        min_requests: int = settings.CIRCUIT_BREAKER_MIN_REQUESTS,
        reset_timeout: int = settings.CIRCUIT_BREAKER_RESET_TIMEOUT_SECONDS,
    ):
        self.redis = redis
        self.provider = provider_name.upper()
        self.window_seconds = window_seconds
        self.failure_threshold = failure_threshold
        self.min_requests = min_requests
        self.reset_timeout = reset_timeout

        self._state_key = f"cb:{self.provider}:state"
        self._opened_at_key = f"cb:{self.provider}:opened_at"
        self._success_key = f"cb:{self.provider}:successes"
        self._failure_key = f"cb:{self.provider}:failures"

    async def get_state(self) -> CircuitState:
        """Evaluates and returns the current circuit state."""
        state = await self.redis.get(self._state_key)
        if not state:
            return CircuitState.CLOSED

        if state == CircuitState.OPEN.value:
            opened_at = await self.redis.get(self._opened_at_key)
            if opened_at:
                elapsed = time.time() - float(opened_at)
                if elapsed >= self.reset_timeout:
                    # Transition to HALF_OPEN probe state
                    await self.redis.set(self._state_key, CircuitState.HALF_OPEN.value)
                    return CircuitState.HALF_OPEN
            return CircuitState.OPEN

        return CircuitState(state)

    async def check_permission(self) -> bool:
        """Returns True if call should proceed, False if circuit is OPEN."""
        state = await self.get_state()
        if state == CircuitState.OPEN:
            return False
        return True

    async def record_success(self) -> None:
        """Records a successful delivery call."""
        state = await self.get_state()
        if state == CircuitState.HALF_OPEN:
            # Probe succeeded: reset to CLOSED
            await self._reset()
            return

        async with self.redis.pipeline() as pipe:
            pipe.incr(self._success_key)
            pipe.expire(self._success_key, self.window_seconds)
            await pipe.execute()

    async def record_failure(self) -> None:
        """Records a failed delivery call and trips circuit if threshold exceeded."""
        state = await self.get_state()
        if state == CircuitState.HALF_OPEN:
            # Probe failed: trip right back to OPEN
            await self._trip()
            return

        async with self.redis.pipeline() as pipe:
            pipe.incr(self._failure_key)
            pipe.expire(self._failure_key, self.window_seconds)
            await pipe.execute()

        # Evaluate failure rate
        successes = int(await self.redis.get(self._success_key) or 0)
        failures = int(await self.redis.get(self._failure_key) or 0)
        total = successes + failures

        if total >= self.min_requests:
            failure_rate = failures / total
            if failure_rate >= self.failure_threshold:
                await self._trip()

    async def _trip(self) -> None:
        """Trips circuit to OPEN state."""
        now = time.time()
        async with self.redis.pipeline() as pipe:
            pipe.set(self._state_key, CircuitState.OPEN.value)
            pipe.set(self._opened_at_key, now)
            await pipe.execute()

    async def _reset(self) -> None:
        """Resets circuit to CLOSED state and clears error counters."""
        async with self.redis.pipeline() as pipe:
            pipe.set(self._state_key, CircuitState.CLOSED.value)
            pipe.delete(self._opened_at_key)
            pipe.delete(self._success_key)
            pipe.delete(self._failure_key)
            await pipe.execute()
