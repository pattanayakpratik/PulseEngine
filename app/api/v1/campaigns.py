import uuid
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import (
    get_db,
    verify_api_key,
    require_idempotency_key,
    get_idempotency_mgr,
)
from app.redis_client import IdempotencyManager
from app.core.segment_compiler import SegmentCompiler
from app.models.segments import Segment
from app.models.users import User
from app.models.outbox import OutboxEvent
from app.models.notification_logs import NotificationLog
from app.schemas.notifications import (
    CampaignDispatchRequest,
    CampaignDispatchResponse,
    DLQReplayResponse,
    PriorityType,
    NotificationStatus,
)

router = APIRouter(prefix="/v1", tags=["Campaigns & DLQ"])


@router.post("/campaigns/dispatch", response_model=CampaignDispatchResponse, status_code=status.HTTP_202_ACCEPTED)
async def dispatch_campaign(
    payload: CampaignDispatchRequest,
    idempotency_key: str = Depends(require_idempotency_key),
    idemp_mgr: IdempotencyManager = Depends(get_idempotency_mgr),
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key),
):
    """
    Triggers a bulk campaign dispatch targeting an entire user segment.
    Uses PostgreSQL streaming cursors to stage outbox events memory-safely.
    """
    acquired, cached_payload = await idemp_mgr.acquire_lock(idempotency_key)
    if not acquired:
        if cached_payload is not None:
            return cached_payload
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Campaign dispatch with this Idempotency-Key is currently in-flight",
        )

    try:
        segment = await db.scalar(select(Segment).where(Segment.id == payload.segment_id))
        if not segment:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Target segment not found")

        # Compile query from segment AST
        user_stmt = SegmentCompiler.compile(segment.filter_ast)

        # Stream users using server-side cursor (yield_per 5000)
        stream = await db.stream_scalars(user_stmt.execution_options(yield_per=5000))
        
        campaign_id = uuid.uuid4()
        now = datetime.now(timezone.utc)
        dispatched_count = 0

        batch_logs = []
        batch_outbox = []

        async for user in stream:
            dispatched_count += 1
            log_id = uuid.uuid4()
            item_idemp_key = f"cmp:{campaign_id}:{user.id}"

            notif_log = NotificationLog(
                id=log_id,
                idempotency_key=item_idemp_key,
                user_id=user.id,
                channel=payload.channel.value,
                priority=PriorityType.BULK.value,
                status=NotificationStatus.QUEUED.value,
                attempt_count=0,
                created_at=now,
                updated_at=now,
            )
            outbox_ev = OutboxEvent(
                aggregate_id=log_id,
                priority_lane=PriorityType.BULK.value,
                payload={
                    "campaign_id": str(campaign_id),
                    "log_id": str(log_id),
                    "idempotency_key": item_idemp_key,
                    "external_user_id": user.external_user_id,
                    "channel": payload.channel.value,
                    "priority": PriorityType.BULK.value,
                    "template_id": payload.template_id,
                    "variables": payload.variables,
                    "user_email": user.email,
                    "user_phone": user.phone_e164,
                    "user_fcm": user.fcm_token,
                },
                status="PENDING",
                created_at=now,
            )

            batch_logs.append(notif_log)
            batch_outbox.append(outbox_ev)

            # Flush periodically and expunge to keep RAM flat without closing server-side cursor
            if len(batch_logs) >= 1000:
                db.add_all(batch_logs)
                db.add_all(batch_outbox)
                await db.flush()
                for item in batch_logs:
                    db.expunge(item)
                for item in batch_outbox:
                    db.expunge(item)
                batch_logs.clear()
                batch_outbox.clear()

        # Flush remaining records
        if batch_logs:
            db.add_all(batch_logs)
            db.add_all(batch_outbox)
            await db.flush()
            for item in batch_logs:
                db.expunge(item)
            for item in batch_outbox:
                db.expunge(item)
            batch_logs.clear()
            batch_outbox.clear()

        # Update segment estimated count and commit transaction once cursor finishes
        await db.execute(
            update(Segment)
            .where(Segment.id == payload.segment_id)
            .values(estimated_count=dispatched_count)
        )
        await db.commit()

        response_data = {
            "campaign_id": str(campaign_id),
            "segment_id": str(payload.segment_id),
            "targeted_count": dispatched_count,
            "status": "QUEUED",
            "queued_at": now.isoformat(),
        }

        await idemp_mgr.commit_response(idempotency_key, response_data)
        return response_data

    except Exception:
        await idemp_mgr.release_lock(idempotency_key)
        raise


@router.post("/dlq/{log_id}/replay", response_model=DLQReplayResponse)
async def replay_dlq_message(
    log_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key),
):
    """
    Manually replays a dead-lettered message from the Next.js control plane.
    Resets status to QUEUED and re-stages an event in the transactional outbox.
    """
    log = await db.scalar(select(NotificationLog).where(NotificationLog.id == log_id))
    if not log:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Notification log not found")

    if log.status != NotificationStatus.DLQ_FAILED.value:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Only messages in DLQ_FAILED status can be replayed. Current status: '{log.status}'",
        )

    now = datetime.now(timezone.utc)
    log.status = NotificationStatus.QUEUED.value
    log.attempt_count = 0
    log.error_message = None
    log.updated_at = now

    user = await db.scalar(select(User).where(User.id == log.user_id)) if log.user_id else None

    # Re-stage outbox event with recipient contact details
    outbox_ev = OutboxEvent(
        aggregate_id=log.id,
        priority_lane=log.priority,
        payload={
            "log_id": str(log.id),
            "idempotency_key": log.idempotency_key,
            "external_user_id": user.external_user_id if user else "unknown",
            "channel": log.channel,
            "priority": log.priority,
            "user_email": user.email if user else None,
            "user_phone": user.phone_e164 if user else None,
            "user_fcm": user.fcm_token if user else None,
            "replayed": True,
        },
        status="PENDING",
        created_at=now,
    )
    db.add(outbox_ev)
    await db.commit()

    return DLQReplayResponse(
        log_id=log.id,
        status="QUEUED",
        replayed_at=now,
        lane=PriorityType(log.priority),
    )
