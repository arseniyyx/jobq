import uuid
from datetime import datetime
from typing import Any, Generic, TypeVar

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field

from app.models import DeliveryStatus, JobStatus

T = TypeVar("T")


class JobCreate(BaseModel):
    type: str
    payload: dict[str, Any] = Field(default_factory=dict)
    webhook_url: AnyHttpUrl | None = None
    max_attempts: int = Field(default=5, ge=1, le=25)
    delay_seconds: float = Field(default=0, ge=0, le=7 * 24 * 3600)


class JobOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    type: str
    payload: dict[str, Any]
    status: JobStatus
    attempts: int
    max_attempts: int
    run_at: datetime
    result: dict[str, Any] | None
    last_error: str | None
    webhook_url: str | None
    created_at: datetime
    finished_at: datetime | None


class DeliveryOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    url: str
    status: DeliveryStatus
    attempts: int
    last_status_code: int | None
    last_error: str | None
    next_attempt_at: datetime
    delivered_at: datetime | None


class Page(BaseModel, Generic[T]):
    items: list[T]
    total: int
    limit: int
    offset: int
