"""Job handlers. Each job type declares a payload schema and an async function.

Adding a new job type is one decorated function; the API validates payloads against
its schema at submit time, so bad input is rejected with 422 instead of failing later.
"""

import asyncio
import hashlib
import re
from collections import Counter
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, Field


class PermanentError(Exception):
    """Raise from a handler when retrying cannot help (bad input, 4xx upstream, ...)."""


@dataclass(frozen=True)
class Handler:
    name: str
    schema: type[BaseModel]
    func: Callable[[Any], Awaitable[dict[str, Any]]]


REGISTRY: dict[str, Handler] = {}


def handler(name: str, schema: type[BaseModel]):
    def decorator(func):
        REGISTRY[name] = Handler(name, schema, func)
        return func

    return decorator


# --- Example handlers ---------------------------------------------------------


class TextStatsPayload(BaseModel):
    text: str = Field(max_length=1_000_000)
    top: int = Field(default=5, ge=1, le=50)


@handler("text_stats", TextStatsPayload)
async def text_stats(p: TextStatsPayload) -> dict[str, Any]:
    words = re.findall(r"\w+", p.text.lower())
    return {
        "characters": len(p.text),
        "words": len(words),
        "unique_words": len(set(words)),
        "top_words": Counter(words).most_common(p.top),
    }


class HashPayload(BaseModel):
    data: str
    iterations: int = Field(default=1, ge=1, le=1_000_000)


@handler("sha256", HashPayload)
async def sha256(p: HashPayload) -> dict[str, Any]:
    def work() -> str:
        digest = p.data.encode()
        for _ in range(p.iterations):
            digest = hashlib.sha256(digest).digest()
        return digest.hex()

    # CPU-bound work goes to a thread so it does not block the worker's event loop.
    return {"sha256": await asyncio.to_thread(work)}


class FetchPayload(BaseModel):
    url: str = Field(pattern=r"^https?://")


@handler("fetch_url", FetchPayload)
async def fetch_url(p: FetchPayload) -> dict[str, Any]:
    import httpx

    async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
        r = await client.get(p.url)
    if 400 <= r.status_code < 500:
        raise PermanentError(f"upstream returned {r.status_code}")
    r.raise_for_status()  # 5xx -> retried
    return {"status_code": r.status_code, "bytes": len(r.content)}


class SleepPayload(BaseModel):
    seconds: float = Field(ge=0, le=300)


@handler("sleep", SleepPayload)
async def sleep(p: SleepPayload) -> dict[str, Any]:
    await asyncio.sleep(p.seconds)
    return {"slept": p.seconds}
