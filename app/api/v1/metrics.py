import asyncio
import json
import time
from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse
from redis.asyncio import Redis
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db, get_redis
from app.models.notification_logs import NotificationLog
from app.models.outbox import OutboxEvent

router = APIRouter(prefix="/v1", tags=["Telemetry & Metrics"])


@router.get("/metrics/summary")
async def get_metrics_summary(
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    """Returns real-time snapshot of system queues, circuit breakers, and delivery health."""
    # Check queue depths in Redis
    critical_depth = await redis.llen("queue:critical")
    bulk_depth = await redis.llen("queue:bulk")
    dlq_depth = await redis.llen("queue:dlq")

    # Fetch circuit breaker states from Redis keys
    providers = ["FCM", "WHATSAPP", "SMTP", "WEBHOOK"]
    cb_states = {}
    for p in providers:
        state = await redis.get(f"cb:{p}:state")
        cb_states[p] = state if state else "CLOSED"

    # Fetch recent delivery stats from PostgreSQL
    total_logs = (await db.scalar(select(func.count(NotificationLog.id)))) or 0
    delivered_count = (await db.scalar(
        select(func.count(NotificationLog.id)).where(NotificationLog.status == "DELIVERED")
    )) or 0
    dlq_count = (await db.scalar(
        select(func.count(NotificationLog.id)).where(NotificationLog.status == "DLQ_FAILED")
    )) or 0
    pending_outbox = (await db.scalar(
        select(func.count(OutboxEvent.id)).where(OutboxEvent.status == "PENDING")
    )) or 0

    avg_latency = (await db.scalar(
        select(func.avg(NotificationLog.latency_ms)).where(NotificationLog.latency_ms.isnot(None))
    )) or 0.0

    return {
        "timestamp": time.time(),
        "queues": {
            "critical_depth": critical_depth,
            "bulk_depth": bulk_depth,
            "dlq_depth": dlq_depth,
            "pending_outbox_events": pending_outbox,
        },
        "circuit_breakers": cb_states,
        "delivery_stats": {
            "total_notifications": total_logs,
            "delivered": delivered_count,
            "dlq_failed": dlq_count,
            "success_rate_percent": round((delivered_count / total_logs * 100), 2) if total_logs > 0 else 100.0,
            "avg_latency_ms": round(float(avg_latency), 1),
        },
    }


@router.get("/stream/telemetry")
async def stream_telemetry(redis: Redis = Depends(get_redis)):
    """Server-Sent Events (SSE) telemetry stream for the Next.js control plane."""

    async def event_generator():
        while True:
            # Gather metrics
            critical_depth = await redis.llen("queue:critical")
            bulk_depth = await redis.llen("queue:bulk")
            dlq_depth = await redis.llen("queue:dlq")

            providers = ["FCM", "WHATSAPP", "SMTP", "WEBHOOK"]
            cb_states = {}
            for p in providers:
                state = await redis.get(f"cb:{p}:state")
                cb_states[p] = state if state else "CLOSED"

            data = {
                "timestamp": time.time(),
                "queue_critical": critical_depth,
                "queue_bulk": bulk_depth,
                "queue_dlq": dlq_depth,
                "circuit_breakers": cb_states,
            }

            yield f"data: {json.dumps(data)}\n\n"
            await asyncio.sleep(1.0)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
