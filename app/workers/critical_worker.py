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
MAX_RETRY_ATTEMPTS = 5


def handle_stop_signals(signum, frame):
    global _running
    logger.info("Termination signal received. Shutting down Critical Worker cleanly...")
    _running = False


async def run_critical_worker():
    global _running
    signal.signal(signal.SIGINT, handle_stop_signals)
    signal.signal(signal.SIGTERM, handle_stop_signals)

    engine = create_async_engine(settings.DATABASE_URL, echo=False)
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

            log_id = item.get("log_id")
            channel = item.get("channel", "FCM").upper()
            attempt = item.get("attempt_count", 0) + 1

            cb = CircuitBreaker(redis, channel)
            is_closed = await cb.check_permission()

            if not is_closed:
                logger.warning("Circuit Breaker OPEN for channel '%s'. Re-queueing critical item %s.", channel, log_id)
                await asyncio.sleep(0.5)
                await redis.rpush("queue:critical", json.dumps(item))
                continue

            # Check Token-Bucket rate limit
            rps_limit = getattr(settings, f"RATE_LIMIT_{channel}_RPS", 100)
            allowed, wait_seconds = await rate_limiter.acquire(channel, rps_limit)
            if not allowed:
                logger.warning("Rate limit hit for %s. Backing off for %.3fs...", channel, wait_seconds)
                await asyncio.sleep(min(wait_seconds, 1.0))
                await redis.rpush("queue:critical", json.dumps(item))
                continue

            # Dispatch via provider adapter
            provider = get_provider(channel)
            start_time = time.perf_counter()
            try:
                result = await provider.send(item)
            except Exception as exc:
                logger.error("Provider execution raised exception: %s", exc)
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
                            # Full Jitter Exponential Backoff
                            t_sleep = random.uniform(0, min(10.0, 0.2 * (2 ** attempt)))
                            item["attempt_count"] = attempt
                            logger.info("Retrying notification %s (attempt %d) in %.2fs...", log_id, attempt, t_sleep)
                            await asyncio.sleep(t_sleep)
                            await redis.rpush("queue:critical", json.dumps(item))

        except Exception as e:
            logger.error("Critical worker loop encounter error: %s", str(e), exc_info=True)
            await asyncio.sleep(0.5)

    await engine.dispose()
    logger.info("Critical Worker stopped.")


if __name__ == "__main__":
    asyncio.run(run_critical_worker())
