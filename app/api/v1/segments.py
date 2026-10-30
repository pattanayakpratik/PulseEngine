import time
import uuid
from typing import List
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db, verify_api_key
from app.core.segment_compiler import SegmentCompiler, SegmentCompilerError
from app.models.segments import Segment
from app.models.users import User
from app.schemas.segments import (
    SegmentCreate,
    SegmentResponse,
    SegmentPreviewRequest,
    SegmentPreviewResponse,
    SampleUser,
)

router = APIRouter(prefix="/v1/segments", tags=["Segments"])


@router.post("", response_model=SegmentResponse, status_code=status.HTTP_201_CREATED)
async def create_segment(
    payload: SegmentCreate,
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key),
):
    """Creates a new dynamic user segment from a JSON AST."""
    # Validate compile-ability
    try:
        SegmentCompiler.compile(payload.filter_ast)
    except SegmentCompilerError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid segment AST: {str(e)}",
        )

    # Check for existing name
    existing = await db.scalar(select(Segment).where(Segment.name == payload.name))
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"A segment with name '{payload.name}' already exists",
        )

    # Calculate initial estimated count
    try:
        stmt = SegmentCompiler.compile(payload.filter_ast)
        count_stmt = select(func.count()).select_from(stmt.subquery())
        estimated_count = (await db.scalar(count_stmt)) or 0
    except Exception:
        estimated_count = 0

    segment = Segment(
        name=payload.name,
        description=payload.description,
        filter_ast=payload.filter_ast.model_dump(),
        estimated_count=estimated_count,
    )
    db.add(segment)
    await db.commit()
    await db.refresh(segment)
    return segment


@router.get("", response_model=List[SegmentResponse])
async def list_segments(
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key),
):
    """Lists all configured segments."""
    result = await db.scalars(select(Segment).order_by(Segment.created_at.desc()))
    return list(result.all())


@router.get("/{segment_id}", response_model=SegmentResponse)
async def get_segment(
    segment_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key),
):
    """Retrieves a single segment by UUID."""
    segment = await db.scalar(select(Segment).where(Segment.id == segment_id))
    if not segment:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Segment not found")
    return segment


@router.post("/preview", response_model=SegmentPreviewResponse)
async def preview_segment(
    payload: SegmentPreviewRequest,
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key),
):
    """
    Simulates AST compilation into parameterized PostgreSQL query.
    Returns matched count, sample users, compiled SQL, and execution latency in ms.
    """
    try:
        compiled_sql = SegmentCompiler.compile_sql_string(payload.filter_ast)
        base_stmt = SegmentCompiler.compile(payload.filter_ast)
    except SegmentCompilerError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Compiler Error: {str(e)}",
        )

    start_time = time.perf_counter()

    # 1. Matched count
    count_stmt = select(func.count()).select_from(base_stmt.subquery())
    count_res = await db.scalar(count_stmt)
    matched_count = count_res or 0

    # 2. Sample users
    sample_stmt = base_stmt.limit(payload.limit)
    sample_res = await db.scalars(sample_stmt)
    users_list = list(sample_res.all())

    duration_ms = round((time.perf_counter() - start_time) * 1000, 2)

    sample_dtos = [
        SampleUser(
            id=u.id,
            external_user_id=u.external_user_id,
            email=u.email,
            phone_e164=u.phone_e164,
            country=u.country,
            last_active_at=u.last_active_at,
            metadata=u.metadata_,
        )
        for u in users_list
    ]

    return SegmentPreviewResponse(
        matched_count=matched_count,
        sample_users=sample_dtos,
        execution_time_ms=duration_ms,
        compiled_sql=compiled_sql,
    )
