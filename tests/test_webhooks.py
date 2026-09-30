import json

import httpx
from httpx import AsyncClient

from app.config import get_settings
from app.db import SessionLocal
from app.webhooks import deliver_one, verify
from app.worker import process_one


class Receiver:
    """Fake webhook endpoint that records requests and returns scripted status codes."""

    def __init__(self, *codes: int) -> None:
        self.codes = list(codes)
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return httpx.Response(self.codes.pop(0) if self.codes else 200)


async def run_deliveries(receiver: Receiver) -> int:
    n = 0
    async with httpx.AsyncClient(transport=httpx.MockTransport(receiver)) as http:
        while True:
            async with SessionLocal() as s:
                if not await deliver_one(s, http):
                    return n
            n += 1


async def test_signed_webhook_on_success(client: AsyncClient) -> None:
    r = await client.post(
        "/jobs",
        json={"type": "count", "payload": {}, "webhook_url": "https://example.com/hook"},
    )
    job_id = r.json()["id"]
    await process_one("w")

    receiver = Receiver(200)
    assert await run_deliveries(receiver) == 1
    req = receiver.requests[0]
    assert str(req.url) == "https://example.com/hook"
    body = json.loads(req.content)
    assert body["event"] == "job.succeeded"
    assert body["job"]["id"] == job_id
    assert verify(
        "test-webhook-secret",
        req.headers["X-Jobq-Timestamp"],
        req.content,
        req.headers["X-Jobq-Signature"],
    )
    assert not verify(
        "wrong-secret",
        req.headers["X-Jobq-Timestamp"],
        req.content,
        req.headers["X-Jobq-Signature"],
    )

    deliveries = (await client.get(f"/jobs/{job_id}/deliveries")).json()
    assert deliveries[0]["status"] == "delivered"


async def test_webhook_retries_then_gives_up(client: AsyncClient) -> None:
    get_settings().webhook_max_attempts = 3
    r = await client.post(
        "/jobs",
        json={
            "type": "permanent_fail",
            "payload": {},
            "webhook_url": "https://example.com/hook",
        },
    )
    job_id = r.json()["id"]
    await process_one("w")

    receiver = Receiver(500, 503, 500, 200)
    assert await run_deliveries(receiver) == 3  # stops at max attempts, never sees the 200
    assert json.loads(receiver.requests[0].content)["event"] == "job.dead"
    [delivery] = (await client.get(f"/jobs/{job_id}/deliveries")).json()
    assert delivery["status"] == "failed"
    assert delivery["attempts"] == 3
    assert delivery["last_status_code"] == 500


async def test_webhook_recovers_after_errors(client: AsyncClient) -> None:
    r = await client.post(
        "/jobs", json={"type": "count", "payload": {}, "webhook_url": "https://example.com/h"}
    )
    await process_one("w")
    receiver = Receiver(500, 200)
    assert await run_deliveries(receiver) == 2
    [delivery] = (await client.get(f"/jobs/{r.json()['id']}/deliveries")).json()
    assert delivery["status"] == "delivered"
    assert delivery["attempts"] == 2


async def test_no_webhook_without_url(client: AsyncClient) -> None:
    await client.post("/jobs", json={"type": "count", "payload": {}})
    await process_one("w")
    assert await run_deliveries(Receiver()) == 0
