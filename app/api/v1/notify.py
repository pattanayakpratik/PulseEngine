import uuid
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import JSONResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import (
    get_db,
    verify_api_key,
    require_idempotency_key,
    get_idempotency_mgr,
)
from app.redis_client import IdempotencyManager
from app.models.users import User
from app.models.outbox import OutboxEvent
from app.models.notification_logs import NotificationLog
from app.schemas.notifications import (
    NotifyRequest,
    NotifyResponse,
    NotificationStatus,
)

router = APIRouter(prefix="/v1/notify", tags=["Transactional Notifications"])


@router.post("", response_model=NotifyResponse, status_code=status.HTTP_202_ACCEPTED)
async def dispatch_notification(
    payload: NotifyRequest,
    idempotency_key: str = Depends(require_idempotency_key),
    idemp_mgr: IdempotencyManager = Depends(get_idempotency_mgr),
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key),
):
    """
    Guaranteed At-Least-Once Delivery dispatch route.
    Guarded by a 24-hour Redis Idempotency Lock and single-transaction PostgreSQL Outbox.
    """
    # 1. Idempotency verification
    acquired, cached_payload = await idemp_mgr.acquire_lock(idempotency_key)
    if not acquired:
        if cached_payload is not None:
            # Replay cached response for completed duplicate request
            return JSONResponse(status_code=status.HTTP_200_OK, content=cached_payload)
        # Concurrent request in flight
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Conflict: A request with Idempotency-Key '{idempotency_key}' is currently in-flight",
        )

    try:
        # 2. Resolve target user
        user = await db.scalar(select(User).where(User.external_user_id == payload.external_user_id))
        user_id = user.id if user else None

        now = datetime.now(timezone.utc)
        log_id = uuid.uuid4()

        # 3. Create Notification Audit Log & Outbox Event in the SAME ACID Transaction
        notif_log = NotificationLog(
            id=log_id,
            idempotency_key=idempotency_key,
            user_id=user_id,
            channel=payload.channel.value,
            priority=payload.priority.value,
            status=NotificationStatus.QUEUED.value,
            attempt_count=0,
            created_at=now,
            updated_at=now,
        )

        outbox_event = OutboxEvent(
            aggregate_id=log_id,
            priority_lane=payload.priority.value,
            payload={
                "log_id": str(log_id),
                "idempotency_key": idempotency_key,
                "external_user_id": payload.external_user_id,
                "channel": payload.channel.value,
                "priority": payload.priority.value,
                "template_id": payload.template_id,
                "variables": payload.variables,
                "user_email": user.email if user else None,
                "user_phone": user.phone_e164 if user else None,
                "user_fcm": user.fcm_token if user else None,
            },
            status="PENDING",
            created_at=now,
        )

        db.add(notif_log)
        db.add(outbox_event)
        await db.commit()

        # 4. Serialize committed response
        response_data = {
            "log_id": str(log_id),
            "idempotency_key": idempotency_key,
            "external_user_id": payload.external_user_id,
            "channel": payload.channel.value,
            "priority": payload.priority.value,
            "status": NotificationStatus.QUEUED.value,
            "created_at": now.isoformat(),
        }

        # 5. Persist cached response in Redis
        await idemp_mgr.commit_response(idempotency_key, response_data)

        return response_data

    except Exception:
        # Release the lock if uncommitted so client can retry cleanly
        await idemp_mgr.release_lock(idempotency_key)
        raise
