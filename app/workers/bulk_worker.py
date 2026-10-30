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
MAX_RETRY_ATTEMPTS = 5
BULK_POLL_BATCH = 50


def handle_stop_signals(signum, frame):
    global _running
    logger.info("Termination signal received. Shutting down Bulk Worker cleanly...")
    _running = False


async def run_bulk_worker():
    global _running
    signal.signal(signal.SIGINT, handle_stop_signals)
    signal.signal(signal.SIGTERM, handle_stop_signals)

    engine = create_async_engine(settings.DATABASE_URL, echo=False)
    session_factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    redis = get_redis_client()
    rate_limiter = RedisTokenBucketRateLimiter(redis)

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

            for item in items:
                log_id = item.get("log_id")
                channel = item.get("channel", "FCM").upper()
                attempt = item.get("attempt_count", 0) + 1

                # Circuit Breaker check
                cb = CircuitBreaker(redis, channel)
                if not (await cb.check_permission()):
                    logger.warning("Circuit Breaker OPEN for channel '%s'. Re-queueing bulk item %s.", channel, log_id)
                    await asyncio.sleep(0.5)
                    await redis.rpush("queue:bulk", json.dumps(item))
                    continue

                # Rate Limiter check
                rps_limit = getattr(settings, f"RATE_LIMIT_{channel}_RPS", 50)
                allowed, wait_seconds = await rate_limiter.acquire(channel, rps_limit)
                if not allowed:
                    await asyncio.sleep(min(wait_seconds, 1.0))
                    await redis.rpush("queue:bulk", json.dumps(item))
                    continue

                # Dispatch via provider
                provider = get_provider(channel)
                start_time = time.perf_counter()
                try:
                    result = await provider.send(item)
                except Exception as exc:
                    logger.error("Bulk delivery error: %s", exc)
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
                                await asyncio.sleep(t_sleep)
                                await redis.rpush("queue:bulk", json.dumps(item))

            logger.info("Processed %d bulk messages batch.", len(items))

        except Exception as e:
            logger.error("Bulk worker error: %s", str(e), exc_info=True)
            await asyncio.sleep(0.5)

    await engine.dispose()
    logger.info("Bulk Worker stopped.")


if __name__ == "__main__":
    asyncio.run(run_bulk_worker())
