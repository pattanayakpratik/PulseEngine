from typing import AsyncGenerator, Optional
from fastapi import Header, HTTPException, status, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis

from app.config import settings
from app.database import AsyncSessionLocal
from app.redis_client import get_redis_client, IdempotencyManager


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


def get_redis() -> Redis:
    return get_redis_client()


def get_idempotency_mgr(redis: Redis = Depends(get_redis)) -> IdempotencyManager:
    return IdempotencyManager(redis)


async def verify_api_key(x_api_key: Optional[str] = Header(None, alias="X-API-Key")) -> str:
    """Validates the client X-API-Key header."""
    if not x_api_key or x_api_key != settings.API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing X-API-Key header",
        )
    return x_api_key


async def require_idempotency_key(idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key")) -> str:
    """Enforces mandatory Idempotency-Key header for mutating routes."""
    if not idempotency_key or not idempotency_key.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Header 'Idempotency-Key' is required for this operation",
        )
    return idempotency_key.strip()
