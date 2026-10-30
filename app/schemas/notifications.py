import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field


class ChannelType(str, Enum):
    FCM = "FCM"
    WHATSAPP = "WHATSAPP"
    EMAIL = "EMAIL"
    WEBHOOK = "WEBHOOK"


class PriorityType(str, Enum):
    CRITICAL = "CRITICAL"
    BULK = "BULK"


class NotificationStatus(str, Enum):
    QUEUED = "QUEUED"
    SENT = "SENT"
    DELIVERED = "DELIVERED"
    RETRYING = "RETRYING"
    DLQ_FAILED = "DLQ_FAILED"


class NotifyRequest(BaseModel):
    external_user_id: str = Field(..., description="Target external user identifier")
    channel: ChannelType = Field(..., description="Delivery channel")
    priority: PriorityType = Field(default=PriorityType.BULK, description="Priority lane")
    template_id: str = Field(..., description="Template identifier")
    variables: Dict[str, Any] = Field(default_factory=dict, description="Variables for template interpolation")


class NotifyResponse(BaseModel):
    log_id: uuid.UUID
    idempotency_key: str
    external_user_id: str
    channel: ChannelType
    priority: PriorityType
    status: NotificationStatus
    created_at: datetime


class CampaignDispatchRequest(BaseModel):
    segment_id: uuid.UUID
    channel: ChannelType
    template_id: str
    variables: Dict[str, Any] = Field(default_factory=dict)


class CampaignDispatchResponse(BaseModel):
    campaign_id: uuid.UUID
    segment_id: uuid.UUID
    targeted_count: int
    status: str
    queued_at: datetime


class DLQReplayResponse(BaseModel):
    log_id: uuid.UUID
    status: str
    replayed_at: datetime
    lane: PriorityType
