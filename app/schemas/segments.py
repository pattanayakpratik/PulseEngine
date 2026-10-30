from __future__ import annotations
import uuid
from datetime import datetime
from typing import Any, Literal, Union, List, Optional
from pydantic import BaseModel, Field, ConfigDict


OperatorType = Literal["eq", "neq", "gt", "gte", "lt", "lte", "in", "contains"]
LogicType = Literal["AND", "OR"]


class SegmentRule(BaseModel):
    field: str = Field(..., description="Field to query: e.g. 'country', 'last_active_at', 'metadata.city', 'metadata.lifetime_spend'")
    op: OperatorType = Field(..., description="Comparison operator: eq, neq, gt, gte, lt, lte, in, contains")
    value: Any = Field(..., description="Value to match against, e.g. 'IN', 5000, ['pro', 'enterprise']")


class SegmentFilterAST(BaseModel):
    logic: LogicType = Field(default="AND", description="Boolean combiner for child rules")
    rules: List[Union[SegmentRule, SegmentFilterAST]] = Field(..., description="List of rules or nested rule groups")


class SegmentCreate(BaseModel):
    name: str = Field(..., min_length=2, max_length=128)
    description: Optional[str] = None
    filter_ast: SegmentFilterAST


class SegmentResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    description: Optional[str] = None
    filter_ast: dict
    estimated_count: int
    created_at: datetime
    updated_at: datetime


class SegmentPreviewRequest(BaseModel):
    filter_ast: SegmentFilterAST
    limit: int = Field(default=10, ge=1, le=100)


class SampleUser(BaseModel):
    id: uuid.UUID
    external_user_id: str
    email: Optional[str] = None
    phone_e164: Optional[str] = None
    country: str
    last_active_at: datetime
    metadata: dict


class SegmentPreviewResponse(BaseModel):
    matched_count: int
    sample_users: List[SampleUser]
    execution_time_ms: float
    compiled_sql: str
