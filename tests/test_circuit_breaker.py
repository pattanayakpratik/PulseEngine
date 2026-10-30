import pytest
import time
from unittest.mock import AsyncMock, MagicMock
from app.core.circuit_breaker import CircuitBreaker, CircuitState


@pytest.mark.asyncio
async def test_circuit_breaker_transitions():
    storage = {}

    # Redis mock: .get, .set, .delete are async methods
    mock_redis = MagicMock()

    async def mock_get(key):
        return storage.get(key)

    async def mock_set(key, val, *args, **kwargs):
        storage[key] = str(val)
        return True

    async def mock_delete(*keys):
        for k in keys:
            storage.pop(k, None)
        return 1

    mock_redis.get = AsyncMock(side_effect=mock_get)
    mock_redis.set = AsyncMock(side_effect=mock_set)
    mock_redis.delete = AsyncMock(side_effect=mock_delete)

    # Setup pipeline mock: in redis-py async, redis.pipeline() is synchronous and returns an async context manager
    pipeline_mock = MagicMock()
    pipeline_mock.__aenter__ = AsyncMock(return_value=pipeline_mock)
    pipeline_mock.__aexit__ = AsyncMock(return_value=None)

    def pipe_set(key, val):
        storage[key] = str(val)

    def pipe_incr(key):
        storage[key] = str(int(storage.get(key, 0)) + 1)

    def pipe_delete(*keys):
        for k in keys:
            storage.pop(k, None)

    pipeline_mock.set = MagicMock(side_effect=pipe_set)
    pipeline_mock.incr = MagicMock(side_effect=pipe_incr)
    pipeline_mock.delete = MagicMock(side_effect=pipe_delete)
    pipeline_mock.expire = MagicMock(return_value=True)
    pipeline_mock.execute = AsyncMock(return_value=[True])

    mock_redis.pipeline = MagicMock(return_value=pipeline_mock)

    cb = CircuitBreaker(
        redis=mock_redis,
        provider_name="SMTP",
        window_seconds=60,
        failure_threshold=0.5,
        min_requests=4,
        reset_timeout=2,
    )

    # Initially closed
    assert await cb.get_state() == CircuitState.CLOSED
    assert await cb.check_permission() is True

    # Record 2 successes, 2 failures (4 requests total, 50% failure rate -> trip)
    await cb.record_success()
    await cb.record_success()
    await cb.record_failure()
    await cb.record_failure()

    # Now circuit must be OPEN
    assert await cb.get_state() == CircuitState.OPEN
    assert await cb.check_permission() is False

    # Simulate timeout elapsed -> transitions to HALF_OPEN
    storage["cb:SMTP:opened_at"] = str(time.time() - 3.0)
    assert await cb.get_state() == CircuitState.HALF_OPEN

    # In HALF_OPEN, a successful probe call resets circuit to CLOSED
    await cb.record_success()
    assert await cb.get_state() == CircuitState.CLOSED


def test_circuit_breaker_email_smtp_mapping():
    mock_redis = MagicMock()
    cb_email = CircuitBreaker(mock_redis, "EMAIL")
    assert cb_email.provider == "SMTP"
    assert cb_email._state_key == "cb:SMTP:state"

