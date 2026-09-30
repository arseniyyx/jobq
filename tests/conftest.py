import os

os.environ.setdefault(
    "DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5432/jobq_test"
)
os.environ["RETRY_BASE_SECONDS"] = "0"  # retries become immediately claimable in tests
os.environ["WEBHOOK_SECRET"] = "test-webhook-secret"

from collections import Counter  # noqa: E402
from collections.abc import AsyncIterator  # noqa: E402

import pytest  # noqa: E402
from alembic import command  # noqa: E402
from alembic.config import Config  # noqa: E402
from httpx import ASGITransport, AsyncClient  # noqa: E402
from pydantic import BaseModel  # noqa: E402
from sqlalchemy import text  # noqa: E402

from app.config import get_settings  # noqa: E402
from app.db import engine  # noqa: E402
from app.handlers import REGISTRY, PermanentError, handler  # noqa: E402
from app.main import app  # noqa: E402

# --- Test-only job types --------------------------------------------------------

CALLS: Counter[str] = Counter()


class KeyPayload(BaseModel):
    key: str = "x"
    fail_times: int = 0


@handler("count", KeyPayload)
async def count(p: KeyPayload) -> dict:
    CALLS[p.key] += 1
    return {"calls": CALLS[p.key]}


@handler("flaky", KeyPayload)
async def flaky(p: KeyPayload) -> dict:
    CALLS[p.key] += 1
    if CALLS[p.key] <= p.fail_times:
        raise RuntimeError(f"boom #{CALLS[p.key]}")
    return {"ok": True}


@handler("permanent_fail", KeyPayload)
async def permanent_fail(p: KeyPayload) -> dict:
    raise PermanentError("bad input, do not retry")


class SlowPayload(BaseModel):
    seconds: float


@handler("slow", SlowPayload)
async def slow(p: SlowPayload) -> dict:
    import asyncio

    await asyncio.sleep(p.seconds)
    return {}


assert {"count", "flaky", "permanent_fail", "slow"} <= set(REGISTRY)


# --- Fixtures -------------------------------------------------------------------


@pytest.fixture(scope="session", autouse=True)
def migrated_database() -> None:
    cfg = Config("alembic.ini")
    cfg.attributes["database_url"] = os.environ["DATABASE_URL"]
    command.downgrade(cfg, "base")
    command.upgrade(cfg, "head")


@pytest.fixture(autouse=True)
async def clean_state() -> AsyncIterator[None]:
    CALLS.clear()
    settings = get_settings()
    snapshot = settings.model_dump()
    yield
    for k, v in snapshot.items():
        setattr(settings, k, v)
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE webhook_deliveries, jobs CASCADE"))


@pytest.fixture
async def client() -> AsyncIterator[AsyncClient]:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c
