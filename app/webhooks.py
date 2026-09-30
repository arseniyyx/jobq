"""Signed webhook delivery with retries.

Receivers verify: hmac_sha256(secret, f"{timestamp}.{raw_body}") == X-Jobq-Signature,
and reject timestamps older than a few minutes to block replays.
"""

import hashlib
import hmac
import json
import time
from datetime import UTC, datetime

import httpx
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.models import DeliveryStatus
from app.queue import backoff, claim_delivery


def sign(secret: str, timestamp: str, body: bytes) -> str:
    mac = hmac.new(secret.encode(), f"{timestamp}.".encode() + body, hashlib.sha256)
    return "sha256=" + mac.hexdigest()


def verify(secret: str, timestamp: str, body: bytes, signature: str, tolerance: int = 300) -> bool:
    if abs(time.time() - int(timestamp)) > tolerance:
        return False
    return hmac.compare_digest(sign(secret, timestamp, body), signature)


async def deliver_one(session: AsyncSession, client: httpx.AsyncClient) -> bool:
    """Attempt one pending delivery. Returns False when there was nothing to do."""
    settings = get_settings()
    delivery = await claim_delivery(session)
    if delivery is None:
        return False

    body = json.dumps(delivery.body, separators=(",", ":")).encode()
    timestamp = str(int(time.time()))
    headers = {
        "Content-Type": "application/json",
        "X-Jobq-Delivery": str(delivery.id),
        "X-Jobq-Timestamp": timestamp,
        "X-Jobq-Signature": sign(settings.webhook_secret, timestamp, body),
    }
    try:
        r = await client.post(
            delivery.url, content=body, headers=headers, timeout=settings.webhook_timeout_seconds
        )
        delivery.last_status_code = r.status_code
        ok = 200 <= r.status_code < 300
        delivery.last_error = None if ok else f"HTTP {r.status_code}"
    except httpx.HTTPError as exc:
        ok = False
        delivery.last_error = f"{type(exc).__name__}: {exc}"[:2000]

    if ok:
        delivery.status = DeliveryStatus.delivered
        delivery.delivered_at = datetime.now(UTC)
    elif delivery.attempts >= settings.webhook_max_attempts:
        delivery.status = DeliveryStatus.failed
    else:
        delivery.next_attempt_at = datetime.now(UTC) + backoff(delivery.attempts)
    await session.commit()
    return True
