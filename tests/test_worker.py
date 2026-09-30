import asyncio
from datetime import UTC, datetime, timedelta

from httpx import AsyncClient
from sqlalchemy import update

from app.config import get_settings
from app.db import SessionLocal
from app.models import Job, JobStatus
from app.queue import requeue_stale
from app.worker import process_one
from tests.conftest import CALLS


async def submit(client: AsyncClient, type_: str, payload: dict | None = None, **kw) -> str:
    r = await client.post("/jobs", json={"type": type_, "payload": payload or {}, **kw})
    assert r.status_code == 202, r.text
    return r.json()["id"]


async def get(client: AsyncClient, job_id: str) -> dict:
    return (await client.get(f"/jobs/{job_id}")).json()


async def drain(worker: str = "w") -> int:
    n = 0
    while await process_one(worker):
        n += 1
    return n


async def test_success(client: AsyncClient) -> None:
    job_id = await submit(client, "text_stats", {"text": "the cat and the hat"})
    assert await drain() == 1
    job = await get(client, job_id)
    assert job["status"] == "succeeded"
    assert job["attempts"] == 1
    assert job["result"]["words"] == 5
    assert job["result"]["top_words"][0] == ["the", 2]


async def test_retries_then_succeeds(client: AsyncClient) -> None:
    job_id = await submit(client, "flaky", {"key": "f", "fail_times": 2}, max_attempts=5)
    await drain()
    job = await get(client, job_id)
    assert job["status"] == "succeeded"
    assert job["attempts"] == 3
    assert CALLS["f"] == 3


async def test_dead_after_max_attempts(client: AsyncClient) -> None:
    job_id = await submit(client, "flaky", {"key": "f", "fail_times": 99}, max_attempts=3)
    await drain()
    job = await get(client, job_id)
    assert job["status"] == "dead"
    assert job["attempts"] == 3
    assert "boom #3" in job["last_error"]

    # Manual retry from the dead-letter state gives it a fresh budget
    assert (await client.post(f"/jobs/{job_id}/retry")).json()["status"] == "queued"


async def test_permanent_error_is_not_retried(client: AsyncClient) -> None:
    job_id = await submit(client, "permanent_fail", max_attempts=5)
    await drain()
    job = await get(client, job_id)
    assert job["status"] == "dead"
    assert job["attempts"] == 1


async def test_timeout(client: AsyncClient) -> None:
    get_settings().job_timeout_seconds = 0.05
    job_id = await submit(client, "slow", {"seconds": 5}, max_attempts=1)
    await drain()
    job = await get(client, job_id)
    assert job["status"] == "dead"
    assert "timed out" in job["last_error"]


async def test_backoff_delays_retry(client: AsyncClient) -> None:
    get_settings().retry_base_seconds = 3600
    job_id = await submit(client, "flaky", {"key": "f", "fail_times": 1})
    assert await drain() == 1  # first attempt fails, retry is scheduled in the future
    job = await get(client, job_id)
    assert job["status"] == "queued"
    assert datetime.fromisoformat(job["run_at"]) > datetime.now(UTC)


async def test_delayed_job_waits(client: AsyncClient) -> None:
    await submit(client, "count", delay_seconds=3600)
    assert await drain() == 0


async def test_each_job_runs_exactly_once_with_concurrent_workers(client: AsyncClient) -> None:
    for i in range(40):
        await submit(client, "count", {"key": f"job{i}"})
    processed = await asyncio.gather(*(drain(f"w{i}") for i in range(8)))
    assert sum(processed) == 40
    assert len(CALLS) == 40
    assert set(CALLS.values()) == {1}  # nobody ran a job twice
    assert sum(1 for n in processed if n) > 1  # work was actually shared


async def test_stale_running_job_is_requeued(client: AsyncClient) -> None:
    job_id = await submit(client, "count")
    async with SessionLocal() as s:
        await s.execute(
            update(Job)
            .where(Job.id == job_id)
            .values(
                status=JobStatus.running,
                locked_by="dead-worker",
                locked_at=datetime.now(UTC) - timedelta(hours=1),
            )
        )
        await s.commit()
        assert await requeue_stale(s) == 1
    assert (await get(client, job_id))["status"] == "queued"
    await drain()
    assert (await get(client, job_id))["status"] == "succeeded"
