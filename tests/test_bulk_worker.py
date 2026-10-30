import asyncio
import json
import uuid
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from app.workers.bulk_worker import (
    process_single_bulk_item,
    schedule_requeue,
    _pending_requeues,
)
from app.providers.base import ProviderResult


@pytest.mark.asyncio
async def test_bulk_worker_concurrent_non_blocking_processing():
    mock_redis = AsyncMock()
    mock_redis.rpush = AsyncMock()

    # Mock session factory
    mock_session = AsyncMock()
    mock_session.execute = AsyncMock()
    mock_session.begin = MagicMock()
    mock_session.begin.return_value.__aenter__ = AsyncMock()
    mock_session.begin.return_value.__aexit__ = AsyncMock()

    mock_session_factory = MagicMock()
    mock_session_factory.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_factory.return_value.__aexit__ = AsyncMock()

    # Mock rate limiter
    mock_rate_limiter = AsyncMock()
    mock_rate_limiter.acquire = AsyncMock(return_value=(True, 0.0))

    item1 = {
        "log_id": str(uuid.uuid4()),
        "channel": "FCM",
        "attempt_count": 0,
        "user_fcm": "token_1",
    }
    item2 = {
        "log_id": str(uuid.uuid4()),
        "channel": "FCM",
        "attempt_count": 0,
        "user_fcm": "token_2",
    }

    # Provider mock: item1 fails, item2 succeeds
    async def mock_send(item):
        if item["log_id"] == item1["log_id"]:
            return ProviderResult(success=False, error="Temporary FCM timeout")
        return ProviderResult(success=True, provider_message_id="msg_123")

    mock_provider = MagicMock()
    mock_provider.send = AsyncMock(side_effect=mock_send)

    with patch("app.workers.bulk_worker.get_provider", return_value=mock_provider), \
         patch("app.workers.bulk_worker.CircuitBreaker") as MockCB:
        cb_instance = AsyncMock()
        cb_instance.check_permission = AsyncMock(return_value=True)
        cb_instance.record_success = AsyncMock()
        cb_instance.record_failure = AsyncMock()
        MockCB.return_value = cb_instance

        # Run both items concurrently with asyncio.gather
        results = await asyncio.gather(
            process_single_bulk_item(item1, mock_redis, mock_session_factory, mock_rate_limiter),
            process_single_bulk_item(item2, mock_redis, mock_session_factory, mock_rate_limiter),
            return_exceptions=True,
        )

        assert results == [None, None]

        # Verify DB updates were executed for successful item
        assert mock_session.execute.call_count == 1
        cb_instance.record_failure.assert_awaited_once()
        cb_instance.record_success.assert_awaited_once()

        # Item 1 should have a pending requeue task scheduled
        assert len(_pending_requeues) > 0

        # Clean up pending tasks
        for t in list(_pending_requeues):
            t.cancel()
        await asyncio.gather(*list(_pending_requeues), return_exceptions=True)


@pytest.mark.asyncio
async def test_schedule_requeue_cancelled_flushes_immediately():
    """Verify that if a container shuts down and cancels the task, the message is preserved in Redis."""
    mock_redis = AsyncMock()
    mock_redis.rpush = AsyncMock()

    item = {"log_id": "test_shutdown_log", "channel": "EMAIL"}

    # Schedule with long 60s delay
    task = schedule_requeue(mock_redis, "queue:bulk", item, delay=60.0)

    # Yield control to let task begin sleeping
    await asyncio.sleep(0)

    # Cancel simulating SIGTERM / container stop
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Verify message was pushed immediately to Redis so it was NOT lost
    mock_redis.rpush.assert_awaited_once_with("queue:bulk", json.dumps(item))
