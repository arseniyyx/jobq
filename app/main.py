import hashlib
import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import Annotated

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Response, status
from pydantic import ValidationError
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_session
from app.handlers import REGISTRY
from app.models import Job, JobStatus, WebhookDelivery
from app.queue import queue_depth
from app.schemas import DeliveryOut, JobCreate, JobOut, Page

app = FastAPI(
    title="jobq",
    description="Background job queue on PostgreSQL with retries and signed webhooks.",
    version="0.1.0",
)

SessionDep = Annotated[AsyncSession, Depends(get_session)]


async def _get_job(session: AsyncSession, job_id: uuid.UUID) -> Job:
    job = await session.get(Job, job_id)
    if job is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Job not found")
    return job


@app.post("/jobs", response_model=JobOut, status_code=status.HTTP_202_ACCEPTED, tags=["jobs"])
async def submit_job(
    body: JobCreate,
    session: SessionDep,
    response: Response,
    idempotency_key: Annotated[str | None, Header(max_length=200)] = None,
) -> Job:
    handler = REGISTRY.get(body.type)
    if handler is None:
        raise HTTPException(422, f"Unknown job type '{body.type}'. Known: {sorted(REGISTRY)}")
    try:
        payload = handler.schema.model_validate(body.payload).model_dump(mode="json")
    except ValidationError as exc:
        raise HTTPException(422, exc.errors(include_url=False)) from None

    request_hash = hashlib.sha256(
        json.dumps(body.model_dump(mode="json"), sort_keys=True).encode()
    ).hexdigest()

    if idempotency_key:
        existing = await session.scalar(select(Job).where(Job.idempotency_key == idempotency_key))
        if existing is not None:
            return _replay(existing, request_hash, response)

    job = Job(
        type=body.type,
        payload=payload,
        max_attempts=body.max_attempts,
        run_at=datetime.now(UTC) + timedelta(seconds=body.delay_seconds),
        webhook_url=str(body.webhook_url) if body.webhook_url else None,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
    )
    session.add(job)
    try:
        await session.commit()
    except IntegrityError:
        # Two requests with the same key raced; the unique index picked a winner.
        await session.rollback()
        existing = await session.scalar(select(Job).where(Job.idempotency_key == idempotency_key))
        if existing is None:
            raise
        return _replay(existing, request_hash, response)
    await session.refresh(job)
    return job


def _replay(existing: Job, request_hash: str, response: Response) -> Job:
    if existing.request_hash != request_hash:
        raise HTTPException(
            status.HTTP_409_CONFLICT, "Idempotency-Key was already used with a different request"
        )
    response.status_code = status.HTTP_200_OK
    response.headers["Idempotent-Replayed"] = "true"
    return existing


@app.get("/jobs", response_model=Page[JobOut], tags=["jobs"])
async def list_jobs(
    session: SessionDep,
    job_status: Annotated[JobStatus | None, Query(alias="status")] = None,
    type: str | None = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> dict:
    query = select(Job)
    if job_status is not None:
        query = query.where(Job.status == job_status)
    if type is not None:
        query = query.where(Job.type == type)
    total = await session.scalar(select(func.count()).select_from(query.subquery()))
    rows = await session.scalars(query.order_by(Job.created_at.desc()).limit(limit).offset(offset))
    return {"items": rows.all(), "total": total, "limit": limit, "offset": offset}


@app.get("/jobs/{job_id}", response_model=JobOut, tags=["jobs"])
async def get_job(job_id: uuid.UUID, session: SessionDep) -> Job:
    return await _get_job(session, job_id)


@app.post("/jobs/{job_id}/cancel", response_model=JobOut, tags=["jobs"])
async def cancel_job(job_id: uuid.UUID, session: SessionDep) -> Job:
    # Conditional update: only succeeds if no worker has claimed the job in the meantime.
    job = await session.scalar(
        Job.__table__.update()
        .where(Job.id == job_id, Job.status == JobStatus.queued)
        .values(status=JobStatus.cancelled, finished_at=datetime.now(UTC))
        .returning(Job.id)
    )
    await session.commit()
    current = await _get_job(session, job_id)
    await session.refresh(current)
    if job is None and current.status != JobStatus.cancelled:
        raise HTTPException(status.HTTP_409_CONFLICT, f"Cannot cancel a {current.status} job")
    return current


@app.post("/jobs/{job_id}/retry", response_model=JobOut, tags=["jobs"])
async def retry_job(job_id: uuid.UUID, session: SessionDep) -> Job:
    """Give a dead (or cancelled) job a fresh set of attempts."""
    job = await _get_job(session, job_id)
    if job.status not in (JobStatus.dead, JobStatus.cancelled):
        raise HTTPException(status.HTTP_409_CONFLICT, f"Cannot retry a {job.status} job")
    job.status = JobStatus.queued
    job.attempts = 0
    job.run_at = datetime.now(UTC)
    job.finished_at = None
    await session.commit()
    await session.refresh(job)
    return job


@app.get("/jobs/{job_id}/deliveries", response_model=list[DeliveryOut], tags=["jobs"])
async def job_deliveries(job_id: uuid.UUID, session: SessionDep) -> list[WebhookDelivery]:
    await _get_job(session, job_id)
    rows = await session.scalars(
        select(WebhookDelivery)
        .where(WebhookDelivery.job_id == job_id)
        .order_by(WebhookDelivery.created_at)
    )
    return list(rows)


@app.get("/job-types", tags=["jobs"])
async def job_types() -> dict[str, dict]:
    return {name: h.schema.model_json_schema() for name, h in sorted(REGISTRY.items())}


@app.get("/stats", tags=["ops"])
async def stats(session: SessionDep) -> dict:
    oldest = await session.scalar(
        select(func.min(Job.run_at)).where(
            Job.status == JobStatus.queued, Job.run_at <= datetime.now(UTC)
        )
    )
    lag = (datetime.now(UTC) - oldest).total_seconds() if oldest else 0.0
    return {"jobs": await queue_depth(session), "oldest_ready_job_age_seconds": round(lag, 3)}


@app.get("/health", tags=["ops"])
async def health(session: SessionDep) -> dict[str, str]:
    await session.execute(text("SELECT 1"))
    return {"status": "ok"}
