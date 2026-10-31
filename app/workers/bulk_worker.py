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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [BulkWorker] %(message)s")
logger = logging.getLogger("BulkWorker")

_running = True
_pending_requeues: set = set()
MAX_RETRY_ATTEMPTS = 5
BULK_POLL_BATCH = 50


def handle_stop_signals(signum, frame):
    global _running
    logger.info("Termination signal received. Shutting down Bulk Worker cleanly...")
    _running = False


def schedule_requeue(redis, queue_name: str, item: dict, delay: float):
    """
    Schedules a non-blocking delayed re-queue so worker loop is not paused.
    If the worker task is cancelled on container shutdown, immediately re-queues
    so items in flight are never lost.
    """
    async def _runner():
        try:
            if delay > 0:
                await asyncio.sleep(delay)
            await redis.rpush(queue_name, json.dumps(item))
        except asyncio.CancelledError:
            # Container shutdown: push immediately to Redis so message is preserved
            await redis.rpush(queue_name, json.dumps(item))
            raise
        except Exception as exc:
            logger.error("Failed to re-queue item %s: %s", item.get("log_id"), exc)
        finally:
            _pending_requeues.discard(task)

    task = asyncio.create_task(_runner())
    _pending_requeues.add(task)
    return task


async def process_single_bulk_item(
    item: dict,
    redis,
    session_factory,
    rate_limiter: RedisTokenBucketRateLimiter,
    db_semaphore: asyncio.Semaphore | None = None,
) -> None:
    log_id = item.get("log_id")
    channel = item.get("channel", "FCM").upper()
    attempt = item.get("attempt_count", 0) + 1

    # Circuit Breaker check
    cb = CircuitBreaker(redis, channel)
    if not (await cb.check_permission()):
        logger.warning("Circuit Breaker OPEN for channel '%s'. Non-blocking re-queue for item %s.", channel, log_id)
        schedule_requeue(redis, "queue:bulk", item, 0.5)
        return

    # Rate Limiter check
    rps_limit = getattr(settings, f"RATE_LIMIT_{channel}_RPS", 50)
    allowed, wait_seconds = await rate_limiter.acquire(channel, rps_limit)
    if not allowed:
        delay = min(wait_seconds, 1.0)
        schedule_requeue(redis, "queue:bulk", item, delay)
        return

    # Dispatch via provider
    provider = get_provider(channel)
    start_time = time.perf_counter()
    try:
        result = await provider.send(item)
    except Exception as exc:
        logger.error("Bulk delivery error for %s: %s", log_id, exc)
        result = None

    duration_ms = int((time.perf_counter() - start_time) * 1000)
    now = datetime.now(timezone.utc)

    async def _update_db():
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
                else:
                    await cb.record_failure()
                    err_msg = result.error if result else "Bulk send exception"

                    if attempt >= MAX_RETRY_ATTEMPTS:
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
                        logger.error("☠️ Bulk notification %s moved to DLQ after %d attempts.", log_id, MAX_RETRY_ATTEMPTS)
                    else:
                        t_sleep = random.uniform(0, min(15.0, 0.5 * (2 ** attempt)))
                        item["attempt_count"] = attempt
                        logger.info("Non-blocking retry scheduled for %s (attempt %d) in %.2fs", log_id, attempt, t_sleep)
                        schedule_requeue(redis, "queue:bulk", item, t_sleep)

    if db_semaphore:
        async with db_semaphore:
            await _update_db()
    else:
        await _update_db()


async def run_bulk_worker():
    global _running
    try:
        signal.signal(signal.SIGINT, handle_stop_signals)
        signal.signal(signal.SIGTERM, handle_stop_signals)
    except (ValueError, AttributeError):
        pass

    engine = create_async_engine(
        settings.DATABASE_URL,
        echo=False,
        pool_size=50,
        max_overflow=20,
        pool_pre_ping=True,
    )
    session_factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    redis = get_redis_client()
    rate_limiter = RedisTokenBucketRateLimiter(redis)
    db_semaphore = asyncio.Semaphore(25)

    logger.info("Bulk Campaign Worker online. Polling 'queue:bulk' in batches of %d...", BULK_POLL_BATCH)

    while _running:
        try:
            # Batch pop from queue:bulk
            async with redis.pipeline() as pipe:
                for _ in range(BULK_POLL_BATCH):
                    pipe.lpop("queue:bulk")
                items_raw = await pipe.execute()

            items = [json.loads(i) for i in items_raw if i is not None]

            if not items:
                await asyncio.sleep(0.5)
                continue

            # Process popped batch concurrently without blocking sequential items
            await asyncio.gather(
                *(process_single_bulk_item(item, redis, session_factory, rate_limiter, db_semaphore) for item in items),
                return_exceptions=True,
            )

            logger.info("Processed %d bulk messages batch concurrently.", len(items))

        except asyncio.CancelledError:
            logger.info("Bulk Campaign Worker task cancelled, shutting down...")
            break
        except Exception as e:
            logger.error("Bulk worker error: %s", str(e), exc_info=True)
            await asyncio.sleep(0.5)

    # Clean shutdown: wait briefly for any pending requeue tasks to flush to Redis
    if _pending_requeues:
        logger.info("Flushing %d pending requeue tasks before shutdown...", len(_pending_requeues))
        for t in list(_pending_requeues):
            t.cancel()
        await asyncio.gather(*list(_pending_requeues), return_exceptions=True)

    await engine.dispose()
    logger.info("Bulk Worker stopped.")


if __name__ == "__main__":
    asyncio.run(run_bulk_worker())
