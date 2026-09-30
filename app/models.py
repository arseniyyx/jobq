import enum
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, Enum, ForeignKey, Index, String, Text, func, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


class JobStatus(enum.StrEnum):
    queued = "queued"
    running = "running"
    succeeded = "succeeded"
    dead = "dead"  # exhausted all attempts
    cancelled = "cancelled"


class DeliveryStatus(enum.StrEnum):
    pending = "pending"
    delivered = "delivered"
    failed = "failed"


def _now_column() -> Mapped[datetime]:
    return mapped_column(DateTime(timezone=True), server_default=func.now())


class Job(Base):
    __tablename__ = "jobs"
    __table_args__ = (
        # The worker's claim query scans exactly this: queued jobs ordered by run_at.
        Index("ix_jobs_ready", "run_at", postgresql_where=text("status = 'queued'")),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    type: Mapped[str] = mapped_column(String(100))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    status: Mapped[JobStatus] = mapped_column(
        Enum(JobStatus, name="job_status"), default=JobStatus.queued, index=True
    )
    attempts: Mapped[int] = mapped_column(default=0)
    max_attempts: Mapped[int] = mapped_column(default=5)
    run_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    locked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    locked_by: Mapped[str | None] = mapped_column(String(100))
    result: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    last_error: Mapped[str | None] = mapped_column(Text)
    webhook_url: Mapped[str | None] = mapped_column(String(2000))
    idempotency_key: Mapped[str | None] = mapped_column(String(200), unique=True)
    request_hash: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = _now_column()
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class WebhookDelivery(Base):
    """Outbox row: written in the same transaction that finishes the job."""

    __tablename__ = "webhook_deliveries"
    __table_args__ = (
        Index(
            "ix_deliveries_ready", "next_attempt_at", postgresql_where=text("status = 'pending'")
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("jobs.id", ondelete="CASCADE"), index=True)
    url: Mapped[str] = mapped_column(String(2000))
    body: Mapped[dict[str, Any]] = mapped_column(JSONB)
    status: Mapped[DeliveryStatus] = mapped_column(
        Enum(DeliveryStatus, name="delivery_status"), default=DeliveryStatus.pending
    )
    attempts: Mapped[int] = mapped_column(default=0)
    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    last_status_code: Mapped[int | None]
    last_error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = _now_column()
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
