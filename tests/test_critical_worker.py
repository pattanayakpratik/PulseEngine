import asyncio
import json
import uuid
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from app.workers.critical_worker import (
    process_single_critical_item,
    schedule_requeue,
    _pending_requeues,
)
from app.providers.base import ProviderResult


@pytest.mark.asyncio
async def test_critical_worker_non_blocking_retry():
    mock_redis = AsyncMock()
    mock_redis.rpush = AsyncMock()

    mock_session = AsyncMock()
    mock_session.execute = AsyncMock()
    mock_session.begin = MagicMock()
    mock_session.begin.return_value.__aenter__ = AsyncMock()
    mock_session.begin.return_value.__aexit__ = AsyncMock()

    mock_session_factory = MagicMock()
    mock_session_factory.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_factory.return_value.__aexit__ = AsyncMock()

    mock_rate_limiter = AsyncMock()
    mock_rate_limiter.acquire = AsyncMock(return_value=(True, 0.0))

    item = {
        "log_id": str(uuid.uuid4()),
        "channel": "FCM",
        "attempt_count": 0,
        "user_fcm": "token_critical_1",
    }

    mock_provider = MagicMock()
    mock_provider.send = AsyncMock(return_value=ProviderResult(success=False, error="Provider network timeout"))

    with patch("app.workers.critical_worker.get_provider", return_value=mock_provider), \
         patch("app.workers.critical_worker.CircuitBreaker") as MockCB:
        cb_instance = AsyncMock()
        cb_instance.check_permission = AsyncMock(return_value=True)
        cb_instance.record_failure = AsyncMock()
        MockCB.return_value = cb_instance

        # Should return immediately without sleeping in the main caller
        await process_single_critical_item(item, mock_redis, mock_session_factory, mock_rate_limiter)

        cb_instance.record_failure.assert_awaited_once()
        assert len(_pending_requeues) > 0

        # Clean up pending tasks
        for t in list(_pending_requeues):
            t.cancel()
        await asyncio.gather(*list(_pending_requeues), return_exceptions=True)


@pytest.mark.asyncio
async def test_critical_schedule_requeue_cancelled_preserves_otp():
    """Verify that cancellation on container stop flushes critical message to Redis."""
    mock_redis = AsyncMock()
    mock_redis.rpush = AsyncMock()

    item = {"log_id": "test_critical_shutdown", "channel": "FCM"}

    task = schedule_requeue(mock_redis, "queue:critical", item, delay=60.0)
    await asyncio.sleep(0)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    mock_redis.rpush.assert_awaited_once_with("queue:critical", json.dumps(item))
