import asyncio

from httpx import AsyncClient


async def test_submit_and_get(client: AsyncClient) -> None:
    r = await client.post("/jobs", json={"type": "text_stats", "payload": {"text": "a b a"}})
    assert r.status_code == 202, r.text
    job = r.json()
    assert job["status"] == "queued"
    assert job["payload"] == {"text": "a b a", "top": 5}  # defaults filled in

    r = await client.get(f"/jobs/{job['id']}")
    assert r.json()["id"] == job["id"]


async def test_unknown_type_and_bad_payload(client: AsyncClient) -> None:
    r = await client.post("/jobs", json={"type": "nope", "payload": {}})
    assert r.status_code == 422
    r = await client.post("/jobs", json={"type": "sleep", "payload": {"seconds": -1}})
    assert r.status_code == 422


async def test_idempotency_key(client: AsyncClient) -> None:
    body = {"type": "count", "payload": {"key": "a"}}
    headers = {"Idempotency-Key": "order-42"}
    first = await client.post("/jobs", json=body, headers=headers)
    second = await client.post("/jobs", json=body, headers=headers)
    assert first.status_code == 202
    assert second.status_code == 200
    assert second.headers["Idempotent-Replayed"] == "true"
    assert first.json()["id"] == second.json()["id"]

    other = await client.post(
        "/jobs", json={"type": "count", "payload": {"key": "b"}}, headers=headers
    )
    assert other.status_code == 409


async def test_idempotency_under_concurrency(client: AsyncClient) -> None:
    body = {"type": "count", "payload": {"key": "a"}}
    results = await asyncio.gather(
        *(client.post("/jobs", json=body, headers={"Idempotency-Key": "k"}) for _ in range(10))
    )
    assert {r.json()["id"] for r in results} == {results[0].json()["id"]}
    assert (await client.get("/jobs")).json()["total"] == 1


async def test_list_filters(client: AsyncClient) -> None:
    for _ in range(3):
        await client.post("/jobs", json={"type": "count", "payload": {}})
    await client.post("/jobs", json={"type": "sleep", "payload": {"seconds": 0}})
    assert (await client.get("/jobs?type=count")).json()["total"] == 3
    assert (await client.get("/jobs?status=queued&limit=2")).json()["total"] == 4
    assert len((await client.get("/jobs?limit=2")).json()["items"]) == 2


async def test_cancel(client: AsyncClient) -> None:
    job = (await client.post("/jobs", json={"type": "count", "payload": {}})).json()
    r = await client.post(f"/jobs/{job['id']}/cancel")
    assert r.json()["status"] == "cancelled"
    assert (await client.post(f"/jobs/{job['id']}/cancel")).status_code == 200  # idempotent
    retried = await client.post(f"/jobs/{job['id']}/retry")
    assert retried.json()["status"] == "queued"


async def test_job_types_and_stats(client: AsyncClient) -> None:
    types = (await client.get("/job-types")).json()
    assert "text_stats" in types and "properties" in types["text_stats"]
    await client.post("/jobs", json={"type": "count", "payload": {}})
    stats = (await client.get("/stats")).json()
    assert stats["jobs"]["queued"] == 1
    assert (await client.get("/health")).json() == {"status": "ok"}
