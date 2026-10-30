import pytest
from unittest.mock import AsyncMock
from app.redis_client import IdempotencyManager


@pytest.mark.asyncio
async def test_idempotency_workflow():
    storage = {}

    mock_redis = AsyncMock()

    async def mock_set(key, val, nx=False, ex=None):
        if nx and key in storage:
            return None
        storage[key] = val
        return True

    async def mock_get(key):
        return storage.get(key)

    async def mock_delete(key):
        storage.pop(key, None)
        return 1

    mock_redis.set.side_effect = mock_set
    mock_redis.get.side_effect = mock_get
    mock_redis.delete.side_effect = mock_delete

    mgr = IdempotencyManager(mock_redis)

    # 1. Fresh key: acquire lock
    acquired, cached = await mgr.acquire_lock("test-key-1")
    assert acquired is True
    assert cached is None
    assert storage["idemp:test-key-1"] == "PROCESSING"

    # 2. Duplicate concurrent request: should return False, None
    acquired_dup, cached_dup = await mgr.acquire_lock("test-key-1")
    assert acquired_dup is False
    assert cached_dup is None

    # 3. Commit response
    response_payload = {"log_id": "123", "status": "QUEUED"}
    await mgr.commit_response("test-key-1", response_payload)

    # 4. Third request: returns cached response
    acquired_replay, cached_replay = await mgr.acquire_lock("test-key-1")
    assert acquired_replay is False
    assert cached_replay == response_payload

    # 5. Release lock on failed request
    await mgr.acquire_lock("test-key-fail")
    assert "idemp:test-key-fail" in storage
    await mgr.release_lock("test-key-fail")
    assert "idemp:test-key-fail" not in storage
