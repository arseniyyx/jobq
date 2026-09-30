# jobq

A background job queue that runs on plain PostgreSQL. No Redis, no RabbitMQ.

You send a job over HTTP, workers pick it up, retry it if it fails, and when it's done
your server gets a signed webhook. FastAPI for the API, asyncio for the workers.

## Why Postgres

Most projects already have a Postgres and already back it up. Adding a broker means one
more thing to deploy, monitor and keep in sync with the database. Keeping jobs in a table
also means you can see what's going on with a normal SQL query.

It won't do tens of thousands of jobs per second. For a typical product backend that's fine.

## How workers share the queue

Every worker runs roughly this:

```sql
UPDATE jobs SET status = 'running', attempts = attempts + 1, ...
WHERE id = (
  SELECT id FROM jobs
  WHERE status = 'queued' AND run_at <= now()
  ORDER BY run_at
  LIMIT 1
  FOR UPDATE SKIP LOCKED
)
RETURNING *;
```

`SKIP LOCKED` is what makes it work. If another worker already locked a row, this one skips
it and takes the next. No two workers get the same job, and nobody waits on anyone.

There's a test that starts 8 workers on 40 jobs and checks that every job ran exactly once.

## What happens to a job

```
queued -> running -> succeeded
             |
             +-> failed? -> queued again later (backoff) -> ... -> dead
```

- Failed jobs are retried with exponential backoff and random jitter, so a flaky
  dependency doesn't get hammered by all retries at once.
- After `max_attempts` the job is marked `dead`. You can look at the error and retry it
  by hand with `POST /jobs/{id}/retry`.
- A handler can raise `PermanentError` when retrying won't help (bad input, a 404 upstream).
  Then the job goes straight to `dead`.
- Each job has a timeout.
- If a worker crashes in the middle of a job, the job stays `running`. A small reaper loop
  notices jobs that have been running too long and puts them back in the queue.
- On SIGTERM the worker stops taking new jobs and finishes the ones it has.

## Idempotency

Send an `Idempotency-Key` header with `POST /jobs`. If the client retries the same request
(timeout, flaky network), it gets the same job back with `200` and
`Idempotent-Replayed: true` instead of creating a duplicate. Same key with a different body
is a `409`. This holds even when the duplicates arrive at the same time, because the key has
a unique index.

## Webhooks

If you pass `webhook_url`, you get a POST when the job succeeds or dies.

The webhook row is written in the same transaction that finishes the job (the outbox
pattern). So there's no case where the job finished but the webhook was lost, or a webhook
went out for a job that didn't actually finish. A separate loop sends them and retries with
backoff if your endpoint is down.

Every request is signed:

```
X-Jobq-Timestamp: 1767225600
X-Jobq-Signature: sha256=<hmac of "timestamp.body" with WEBHOOK_SECRET>
```

To verify on your side:

```python
from app.webhooks import verify

ok = verify(secret, headers["X-Jobq-Timestamp"], raw_body, headers["X-Jobq-Signature"])
```

It uses a constant-time compare and rejects timestamps older than 5 minutes, so an old
request can't be replayed.

## Running it

```bash
docker compose up --build
```

That starts Postgres, runs migrations, starts the API on :8000 and two workers.

```bash
curl -X POST localhost:8000/jobs \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: demo-1' \
  -d '{"type": "text_stats",
       "payload": {"text": "the quick brown fox jumps over the lazy dog"},
       "webhook_url": "https://webhook.site/your-id"}'

curl localhost:8000/jobs/<id>
```

## Endpoints

| Method | Path | What |
|---|---|---|
| POST | `/jobs` | submit a job (`202`) |
| GET | `/jobs` | list, filter by `status` / `type` |
| GET | `/jobs/{id}` | status, attempts, result, last error |
| POST | `/jobs/{id}/cancel` | cancel if it hasn't started |
| POST | `/jobs/{id}/retry` | requeue a dead or cancelled job |
| GET | `/jobs/{id}/deliveries` | webhook attempts for a job |
| GET | `/job-types` | available job types and their payload schemas |
| GET | `/stats` | counts by status + how old the oldest waiting job is |
| GET | `/health` | checks the DB too |

## Adding a job type

Job types live in [`app/handlers.py`](app/handlers.py). One schema, one function:

```python
class ResizePayload(BaseModel):
    image_url: str
    width: int = Field(gt=0, le=4000)


@handler("resize_image", ResizePayload)
async def resize_image(p: ResizePayload) -> dict:
    ...
    return {"url": new_url}
```

The API checks the payload against the schema when the job is submitted, so bad input fails
right away with `422` and not an hour later in a worker.

The built-in examples are `text_stats`, `sha256` (CPU-heavy, runs in a thread so it doesn't
block the event loop), `fetch_url` (4xx is permanent, 5xx gets retried) and `sleep`.

## Local development

You need Python 3.11+, [uv](https://docs.astral.sh/uv/) and Postgres 16.

```bash
uv sync
createdb jobq_test
DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/jobq_test uv run pytest
uv run ruff check . && uv run ruff format --check .

uv run alembic upgrade head
uv run uvicorn app.main:app --reload
uv run python -m app.worker
```

Tests cover retries, backoff, dead jobs, timeouts, crash recovery, concurrent duplicate
submits, webhook signing and retries, and the 8-workers-40-jobs race.

Settings are env variables, see [`.env.example`](.env.example).

## What I'd add next

- Priorities and separate named queues
- Scheduled / cron jobs
- Prometheus metrics instead of the `/stats` endpoint
- `LISTEN/NOTIFY` so idle workers wake up instantly instead of polling
