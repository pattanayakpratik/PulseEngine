import asyncio
import json
import logging
import signal
from datetime import datetime, timezone
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

from app.config import settings
from app.redis_client import get_redis_client
from app.models.outbox import OutboxEvent

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [OutboxRelay] %(message)s")
logger = logging.getLogger("OutboxRelay")

_running = True


def handle_stop_signals(signum, frame):
    global _running
    logger.info("Termination signal received. Shutting down Outbox Relay cleanly...")
    _running = False


async def run_outbox_relay():
    global _running
    signal.signal(signal.SIGINT, handle_stop_signals)
    signal.signal(signal.SIGTERM, handle_stop_signals)

    engine = create_async_engine(settings.DATABASE_URL, echo=False)
    session_factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    redis = get_redis_client()

    logger.info("Outbox Relay Daemon started (interval: %sms, batch: %s)...", settings.OUTBOX_POLL_INTERVAL_MS, settings.OUTBOX_BATCH_SIZE)

    while _running:
        try:
            async with session_factory() as session:
                async with session.begin():
                    # SELECT ... FOR UPDATE SKIP LOCKED
                    query = (
                        select(OutboxEvent)
                        .where(OutboxEvent.status == "PENDING")
                        .order_by(OutboxEvent.created_at.asc())
                        .limit(settings.OUTBOX_BATCH_SIZE)
                        .with_for_update(skip_locked=True)
                    )
                    result = await session.execute(query)
                    events = result.scalars().all()

                    if events:
                        async with redis.pipeline(transaction=True) as pipe:
                            now = datetime.now(timezone.utc)
                            processed_ids = []

                            for event in events:
                                payload_str = json.dumps(event.payload)
                                if event.priority_lane == "CRITICAL":
                                    pipe.rpush("queue:critical", payload_str)
                                else:
                                    pipe.rpush("queue:bulk", payload_str)
                                processed_ids.append(event.id)

                            await pipe.execute()

                            # Mark rows PROCESSED
                            await session.execute(
                                update(OutboxEvent)
                                .where(OutboxEvent.id.in_(processed_ids))
                                .values(status="PROCESSED", processed_at=now)
                            )

                        logger.info("Relayed %d outbox events to Redis queues.", len(events))

            await asyncio.sleep(settings.OUTBOX_POLL_INTERVAL_MS / 1000.0)

        except Exception as e:
            logger.error("Error during outbox sweep: %s", str(e), exc_info=True)
            await asyncio.sleep(1.0)

    await engine.dispose()
    logger.info("Outbox Relay Daemon stopped.")


if __name__ == "__main__":
    asyncio.run(run_outbox_relay())
