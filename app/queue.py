"""Queue primitives shared by the API and the worker.

Claiming uses `FOR UPDATE SKIP LOCKED`: many workers can poll the same table and each
row is handed to exactly one of them, without a separate broker.
"""

import random
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.models import DeliveryStatus, Job, JobStatus, WebhookDelivery


def backoff(attempt: int) -> timedelta:
    """Exponential backoff with full jitter: random(0, base * 2^(attempt-1)), capped."""
    s = get_settings()
    ceiling = min(s.retry_max_seconds, s.retry_base_seconds * 2 ** (attempt - 1))
    return timedelta(seconds=random.uniform(0, ceiling))


async def claim_job(session: AsyncSession, worker_id: str) -> Job | None:
    ready = (
        select(Job.id)
        .where(Job.status == JobStatus.queued, Job.run_at <= datetime.now(UTC))
        .order_by(Job.run_at)
        .limit(1)
        .with_for_update(skip_locked=True)
        .scalar_subquery()
    )
    job = await session.scalar(
        update(Job)
        .where(Job.id == ready)
        .values(
            status=JobStatus.running,
            attempts=Job.attempts + 1,
            locked_at=datetime.now(UTC),
            locked_by=worker_id,
        )
        .returning(Job)
    )
    await session.commit()
    return job


def _webhook_body(job: Job) -> dict[str, Any]:
    return {
        "event": f"job.{job.status.value}",
        "job": {
            "id": str(job.id),
            "type": job.type,
            "status": job.status.value,
            "attempts": job.attempts,
            "result": job.result,
            "error": job.last_error,
        },
    }


def _enqueue_webhook(session: AsyncSession, job: Job) -> None:
    if job.webhook_url:
        session.add(WebhookDelivery(job_id=job.id, url=job.webhook_url, body=_webhook_body(job)))


async def complete_job(session: AsyncSession, job: Job, result: dict[str, Any]) -> None:
    job.status = JobStatus.succeeded
    job.result = result
    job.last_error = None
    job.finished_at = datetime.now(UTC)
    job.locked_at = job.locked_by = None
    _enqueue_webhook(session, job)  # same transaction: no lost or phantom webhooks
    await session.commit()


async def fail_job(session: AsyncSession, job: Job, error: str, permanent: bool = False) -> None:
    job.last_error = error[:5000]
    job.locked_at = job.locked_by = None
    if permanent or job.attempts >= job.max_attempts:
        job.status = JobStatus.dead
        job.finished_at = datetime.now(UTC)
        _enqueue_webhook(session, job)
    else:
        job.status = JobStatus.queued
        job.run_at = datetime.now(UTC) + backoff(job.attempts)
    await session.commit()


async def requeue_stale(session: AsyncSession) -> int:
    """Return jobs abandoned by crashed workers to the queue."""
    cutoff = datetime.now(UTC) - timedelta(seconds=get_settings().visibility_timeout_seconds)
    result = await session.execute(
        update(Job)
        .where(Job.status == JobStatus.running, Job.locked_at < cutoff)
        .values(
            status=JobStatus.queued,
            locked_at=None,
            locked_by=None,
            run_at=datetime.now(UTC),
            last_error="worker lost (visibility timeout)",
        )
    )
    await session.commit()
    return result.rowcount


async def claim_delivery(session: AsyncSession) -> WebhookDelivery | None:
    ready = (
        select(WebhookDelivery.id)
        .where(
            WebhookDelivery.status == DeliveryStatus.pending,
            WebhookDelivery.next_attempt_at <= datetime.now(UTC),
        )
        .order_by(WebhookDelivery.next_attempt_at)
        .limit(1)
        .with_for_update(skip_locked=True)
        .scalar_subquery()
    )
    # Push next_attempt_at forward while we work on it, so a crash mid-delivery just
    # means another dispatcher retries it later.
    lease = datetime.now(UTC) + timedelta(seconds=get_settings().webhook_timeout_seconds * 3)
    delivery = await session.scalar(
        update(WebhookDelivery)
        .where(WebhookDelivery.id == ready)
        .values(attempts=WebhookDelivery.attempts + 1, next_attempt_at=lease)
        .returning(WebhookDelivery)
    )
    await session.commit()
    return delivery


async def queue_depth(session: AsyncSession) -> dict[str, int]:
    rows = await session.execute(text("SELECT status::text, count(*) FROM jobs GROUP BY status"))
    counts = {s.value: 0 for s in JobStatus}
    counts.update({status: n for status, n in rows})
    return counts
