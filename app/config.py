from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    ENVIRONMENT: str = "development"
    LOG_LEVEL: str = "INFO"

    # Security
    API_KEY: str = "pulse_live_test_api_key_884920"
    IDEMPOTENCY_EXPIRE_SECONDS: int = 86400

    # PostgreSQL Database
    DATABASE_URL: str = "postgresql+asyncpg://localhost:5432/pulse_engine"
    DATABASE_URL_SYNC: str = "postgresql://localhost:5432/pulse_engine"

    # Redis Broker
    REDIS_URL: str = "redis://localhost:6379/0"

    # Rate Limiting (Tokens/second)
    RATE_LIMIT_FCM_RPS: int = 500
    RATE_LIMIT_WHATSAPP_RPS: int = 80
    RATE_LIMIT_SMTP_RPS: int = 50
    RATE_LIMIT_WEBHOOK_RPS: int = 200

    # Circuit Breaker Configuration
    CIRCUIT_BREAKER_WINDOW_SECONDS: int = 60
    CIRCUIT_BREAKER_FAILURE_THRESHOLD: float = 0.5
    CIRCUIT_BREAKER_MIN_REQUESTS: int = 20
    CIRCUIT_BREAKER_RESET_TIMEOUT_SECONDS: int = 30

    # Worker Tuning
    OUTBOX_BATCH_SIZE: int = 500
    OUTBOX_POLL_INTERVAL_MS: int = 100
    BULK_STREAM_BATCH_SIZE: int = 5000

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
