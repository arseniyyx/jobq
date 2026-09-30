"""initial schema: jobs and webhook deliveries

Revision ID: 0001
Revises:
Create Date: 2026-09-30
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

job_status = postgresql.ENUM(
    "queued", "running", "succeeded", "dead", "cancelled", name="job_status", create_type=False
)
delivery_status = postgresql.ENUM(
    "pending", "delivered", "failed", name="delivery_status", create_type=False
)


def _ts(name: str, **kw) -> sa.Column:
    return sa.Column(name, sa.DateTime(timezone=True), **kw)


def upgrade() -> None:
    job_status.create(op.get_bind(), checkfirst=True)
    delivery_status.create(op.get_bind(), checkfirst=True)

    op.create_table(
        "jobs",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("type", sa.String(100), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("status", job_status, nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("max_attempts", sa.Integer(), nullable=False),
        _ts("run_at", server_default=sa.func.now(), nullable=False),
        _ts("locked_at"),
        sa.Column("locked_by", sa.String(100)),
        sa.Column("result", postgresql.JSONB()),
        sa.Column("last_error", sa.Text()),
        sa.Column("webhook_url", sa.String(2000)),
        sa.Column("idempotency_key", sa.String(200), unique=True),
        sa.Column("request_hash", sa.String(64)),
        _ts("created_at", server_default=sa.func.now(), nullable=False),
        _ts("finished_at"),
    )
    op.create_index("ix_jobs_status", "jobs", ["status"])
    op.create_index(
        "ix_jobs_ready", "jobs", ["run_at"], postgresql_where=sa.text("status = 'queued'")
    )

    op.create_table(
        "webhook_deliveries",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "job_id", sa.Uuid(), sa.ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False
        ),
        sa.Column("url", sa.String(2000), nullable=False),
        sa.Column("body", postgresql.JSONB(), nullable=False),
        sa.Column("status", delivery_status, nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        _ts("next_attempt_at", server_default=sa.func.now(), nullable=False),
        sa.Column("last_status_code", sa.Integer()),
        sa.Column("last_error", sa.Text()),
        _ts("created_at", server_default=sa.func.now(), nullable=False),
        _ts("delivered_at"),
    )
    op.create_index("ix_webhook_deliveries_job_id", "webhook_deliveries", ["job_id"])
    op.create_index(
        "ix_deliveries_ready",
        "webhook_deliveries",
        ["next_attempt_at"],
        postgresql_where=sa.text("status = 'pending'"),
    )


def downgrade() -> None:
    op.drop_table("webhook_deliveries")
    op.drop_table("jobs")
    delivery_status.drop(op.get_bind(), checkfirst=True)
    job_status.drop(op.get_bind(), checkfirst=True)
