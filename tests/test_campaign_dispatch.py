import uuid
import pytest
from unittest.mock import AsyncMock, MagicMock
from app.api.v1.campaigns import dispatch_campaign
from app.schemas.notifications import CampaignDispatchRequest, PriorityType


@pytest.mark.asyncio
async def test_campaign_dispatch_flushes_without_midstream_commit():
    mock_db = AsyncMock()

    segment_id = uuid.uuid4()
    mock_segment = MagicMock()
    mock_segment.id = segment_id
    mock_segment.filter_ast = {
        "logic": "AND",
        "rules": [{"field": "country", "op": "eq", "value": "IN"}],
    }

    # Simulate 1500 users to trigger the 1000-item flush threshold
    user_list = []
    for i in range(1500):
        u = MagicMock()
        u.id = uuid.uuid4()
        u.email = f"user{i}@example.com"
        u.phone_e164 = "+1234567890"
        u.fcm_token = "token"
        user_list.append(u)

    async def user_generator():
        for u in user_list:
            yield u

    mock_db.scalar = AsyncMock(return_value=mock_segment)
    mock_db.stream_scalars = AsyncMock(return_value=user_generator())
    mock_db.add_all = MagicMock()
    mock_db.flush = AsyncMock()
    mock_db.expunge = MagicMock()
    mock_db.execute = AsyncMock()
    mock_db.commit = AsyncMock()

    mock_idemp_mgr = AsyncMock()
    mock_idemp_mgr.acquire_lock = AsyncMock(return_value=(True, None))
    mock_idemp_mgr.commit_response = AsyncMock()

    req = CampaignDispatchRequest(
        segment_id=segment_id,
        template_id="promo_template",
        priority=PriorityType.BULK,
        channel="EMAIL",
        variables={"discount": 20},
    )

    resp = await dispatch_campaign(
        payload=req,
        idempotency_key="idemp_campaign_1",
        db=mock_db,
        idemp_mgr=mock_idemp_mgr,
        _="test_key",
    )

    assert resp["targeted_count"] == 1500
    assert resp["status"] == "QUEUED"

    # Verify flush was called twice (at 1000 items and for remaining 500)
    assert mock_db.flush.call_count == 2
    # Verify expunge was called for all items to keep memory flat
    assert mock_db.expunge.call_count == 3000  # 1500 logs + 1500 outbox events

    # Verify commit was called exactly ONCE at the end, not mid-stream
    assert mock_db.commit.call_count == 1
