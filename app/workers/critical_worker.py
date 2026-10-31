import asyncio
import json
import logging
import random
import signal
import time
from datetime import datetime, timezone
from sqlalchemy import update
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

from app.config import settings
from app.redis_client import get_redis_client
from app.core.rate_limiter import RedisTokenBucketRateLimiter
from app.core.circuit_breaker import CircuitBreaker
from app.models.notification_logs import NotificationLog
from app.providers import get_provider

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [CriticalWorker] %(message)s")
logger = logging.getLogger("CriticalWorker")

_running = True
_pending_requeues: set = set()
MAX_RETRY_ATTEMPTS = 5


def handle_stop_signals(signum, frame):
    global _running
    logger.info("Termination signal received. Shutting down Critical Worker cleanly...")
    _running = False


def schedule_requeue(redis, queue_name: str, item: dict, delay: float):
    """
    Schedules a non-blocking delayed re-queue so critical worker loop does not stall.
    On container termination (CancelledError), immediately pushes to Redis so OTPs
    in-flight are preserved.
    """
    async def _runner():
        try:
            if delay > 0:
                await asyncio.sleep(delay)
            await redis.rpush(queue_name, json.dumps(item))
        except asyncio.CancelledError:
            await redis.rpush(queue_name, json.dumps(item))
            raise
        except Exception as exc:
            logger.error("Failed to re-queue critical item %s: %s", item.get("log_id"), exc)
        finally:
            _pending_requeues.discard(task)

    task = asyncio.create_task(_runner())
    _pending_requeues.add(task)
    return task


async def process_single_critical_item(
    item: dict,
    redis,
    session_factory,
    rate_limiter: RedisTokenBucketRateLimiter,
) -> None:
    log_id = item.get("log_id")
    channel = item.get("channel", "FCM").upper()
    attempt = item.get("attempt_count", 0) + 1

    cb = CircuitBreaker(redis, channel)
    is_closed = await cb.check_permission()

    if not is_closed:
        logger.warning("Circuit Breaker OPEN for channel '%s'. Non-blocking re-queue for critical item %s.", channel, log_id)
        schedule_requeue(redis, "queue:critical", item, 0.5)
        return

    # Check Token-Bucket rate limit
    rps_limit = getattr(settings, f"RATE_LIMIT_{channel}_RPS", 100)
    allowed, wait_seconds = await rate_limiter.acquire(channel, rps_limit)
    if not allowed:
        delay = min(wait_seconds, 1.0)
        logger.warning("Rate limit hit for %s. Non-blocking backoff for %.3fs...", channel, delay)
        schedule_requeue(redis, "queue:critical", item, delay)
        return

    # Dispatch via provider adapter
    provider = get_provider(channel)
    start_time = time.perf_counter()
    try:
        result = await provider.send(item)
    except Exception as exc:
        logger.error("Provider execution raised exception for %s: %s", log_id, exc)
        result = None

    duration_ms = int((time.perf_counter() - start_time) * 1000)
    now = datetime.now(timezone.utc)

    async with session_factory() as session:
        async with session.begin():
            if result and result.success:
                await cb.record_success()
                await session.execute(
                    update(NotificationLog)
                    .where(NotificationLog.id == log_id)
                    .values(
                        status="DELIVERED",
                        attempt_count=attempt,
                        latency_ms=duration_ms,
                        provider_response=result.to_dict(),
                        updated_at=now,
                    )
                )
                logger.info("⚡ Critical notification %s dispatched via %s in %dms.", log_id, channel, duration_ms)
            else:
                await cb.record_failure()
                err_msg = result.error if result else "Internal delivery error"

                if attempt >= MAX_RETRY_ATTEMPTS:
                    # Route to DLQ
                    item["attempt_count"] = attempt
                    item["error"] = err_msg
                    await redis.rpush("queue:dlq", json.dumps(item))

                    await session.execute(
                        update(NotificationLog)
                        .where(NotificationLog.id == log_id)
                        .values(
                            status="DLQ_FAILED",
                            attempt_count=attempt,
                            error_message=err_msg,
                            updated_at=now,
                        )
                    )
                    logger.error("☠️ Notification %s exceeded %d attempts. Moved to DLQ.", log_id, MAX_RETRY_ATTEMPTS)
                else:
                    # Non-blocking Full Jitter Exponential Backoff
                    t_sleep = random.uniform(0, min(10.0, 0.2 * (2 ** attempt)))
                    item["attempt_count"] = attempt
                    logger.info("Non-blocking retry scheduled for critical %s (attempt %d) in %.2fs...", log_id, attempt, t_sleep)
                    schedule_requeue(redis, "queue:critical", item, t_sleep)


async def run_critical_worker():
    global _running
    try:
        signal.signal(signal.SIGINT, handle_stop_signals)
        signal.signal(signal.SIGTERM, handle_stop_signals)
    except (ValueError, AttributeError):
        pass

    engine = create_async_engine(
        settings.DATABASE_URL,
        echo=False,
        pool_size=20,
        max_overflow=10,
        pool_pre_ping=True,
    )
    session_factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    redis = get_redis_client()
    rate_limiter = RedisTokenBucketRateLimiter(redis)

    logger.info("High-Priority Critical Worker online. Polling 'queue:critical' (SLA: p99 < 500ms)...")

    while _running:
        try:
            # BLPOP with 1 second timeout
            popped = await redis.blpop(["queue:critical"], timeout=1)
            if not popped:
                continue

            _, item_str = popped
            item = json.loads(item_str)

            # Process without blocking the next OTP in queue
            await process_single_critical_item(item, redis, session_factory, rate_limiter)

        except asyncio.CancelledError:
            logger.info("Critical Worker task cancelled, shutting down...")
            break
        except Exception as e:
            logger.error("Critical worker loop encounter error: %s", str(e), exc_info=True)
            await asyncio.sleep(0.5)

    # Clean shutdown: wait briefly for any pending requeue tasks to flush to Redis
    if _pending_requeues:
        logger.info("Flushing %d pending critical requeue tasks before shutdown...", len(_pending_requeues))
        for t in list(_pending_requeues):
            t.cancel()
        await asyncio.gather(*list(_pending_requeues), return_exceptions=True)

    await engine.dispose()
    logger.info("Critical Worker stopped.")


if __name__ == "__main__":
    asyncio.run(run_critical_worker())
