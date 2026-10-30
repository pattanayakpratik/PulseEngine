import uuid
from datetime import datetime
from sqlalchemy import String, Integer, SmallInteger, Text, DateTime, ForeignKey, Index, text
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.database import Base


class NotificationLog(Base):
    __tablename__ = "notification_logs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
    )
    idempotency_key: Mapped[str] = mapped_column(String(128), unique=True, nullable=False, index=True)
    user_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    channel: Mapped[str] = mapped_column(String(16), nullable=False)  # 'FCM', 'WHATSAPP', 'EMAIL', 'WEBHOOK'
    priority: Mapped[str] = mapped_column(String(16), nullable=False)  # 'CRITICAL', 'BULK'
    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="QUEUED",
        server_default="QUEUED",
    )  # 'QUEUED', 'SENT', 'DELIVERED', 'RETRYING', 'DLQ_FAILED'
    attempt_count: Mapped[int] = mapped_column(SmallInteger, default=0, server_default="0")
    provider_response: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    latency_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=datetime.utcnow,
        server_default=text("NOW()"),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        server_default=text("NOW()"),
    )

    __table_args__ = (
        Index("idx_notif_logs_status_channel", "status", "channel", "created_at"),
    )
