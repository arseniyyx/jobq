# jobq

A background job queue built on **PostgreSQL**, with a **FastAPI** HTTP API, an async worker,
retries with exponential backoff, a dead-letter state, and **HMAC-signed webhooks**.

No Redis, no RabbitMQ: the queue is a table, and `SELECT ... FOR UPDATE SKIP LOCKED` lets any
number of workers pull from it without handing the same job to two of them.

![CI](../../actions/workflows/ci.yml/badge.svg)

## What it does

```
client ──POST /jobs──▶ API ──INSERT──▶ jobs table ◀──claim (SKIP LOCKED)── worker × N
                                            │                                  │
                                            │    finish job + write outbox row │
                                            ▼    in ONE transaction  ◀─────────┘
                                   webhook_deliveries ──signed POST──▶ your endpoint
```

- **Submit** a job with a type and payload. Payloads are validated against the job type's
  schema at submit time, so bad input is a `422`, not a failure an hour later.
- **Idempotency-Key** header: retrying the same request returns the same job (`200` plus
  `Idempotent-Replayed: true`); reusing a key with a different body is a `409`. Safe under
  concurrent duplicates thanks to a unique index.
- **Workers** claim jobs with `FOR UPDATE SKIP LOCKED`, run them with a timeout, and on failure
  reschedule with exponential backoff and full jitter. After `max_attempts` the job goes
  **dead** and can be retried manually. Handlers can raise `PermanentError` to skip retries.
- **Crash recovery**: a job stuck in `running` longer than the visibility timeout is returned
  to the queue by a reaper loop.
- **Webhooks** use the transactional outbox pattern: the delivery row is written in the same
  transaction that finishes the job, so a webhook is never lost or sent for a job that did not
  finish. Deliveries retry with backoff and are signed with HMAC-SHA256.
- **Graceful shutdown**: on SIGTERM workers stop claiming and finish in-flight jobs.
- **Ops endpoints**: `/stats` (counts by status plus age of the oldest ready job), `/health`.

## Run it

```bash
docker compose up --build        # Postgres, migrations, API on :8000, two workers
open http://localhost:8000/docs
```

```bash
curl -X POST localhost:8000/jobs \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: demo-1' \
  -d '{"type": "text_stats",
       "payload": {"text": "the quick brown fox jumps over the lazy dog the end"},
       "webhook_url": "https://webhook.site/your-id"}'

curl localhost:8000/jobs/<id>
```

## API

| Method | Path | Description |
|---|---|---|
| POST | `/jobs` | Submit a job (`202`); supports `Idempotency-Key`, `delay_seconds`, `max_attempts`, `webhook_url` |
| GET | `/jobs` | List jobs, filter by `status` and `type`, paginated |
| GET | `/jobs/{id}` | Job status, attempts, result, last error |
| POST | `/jobs/{id}/cancel` | Cancel a job that has not started yet |
| POST | `/jobs/{id}/retry` | Requeue a dead or cancelled job with a fresh attempt budget |
| GET | `/jobs/{id}/deliveries` | Webhook delivery attempts for a job |
| GET | `/job-types` | Registered job types and their JSON schemas |
| GET | `/stats` | Queue depth by status and queue lag |
| GET | `/health` | Liveness plus DB check |

## Job types

Handlers live in [`app/handlers.py`](app/handlers.py). Adding one is a schema plus a function:

```python
class ResizePayload(BaseModel):
    image_url: str
    width: int = Field(gt=0, le=4000)

@handler("resize_image", ResizePayload)
async def resize_image(p: ResizePayload) -> dict:
    ...
    return {"url": new_url}
```

Built-in examples: `text_stats`, `sha256` (CPU-bound, runs in a thread), `fetch_url`
(4xx is permanent, 5xx is retried), `sleep`.

## Verifying webhooks

Each delivery carries `X-Jobq-Delivery`, `X-Jobq-Timestamp` and
`X-Jobq-Signature: sha256=<hex>`, where the signature is
`HMAC_SHA256(WEBHOOK_SECRET, "{timestamp}.{raw_body}")`.

```python
from app.webhooks import verify

ok = verify(secret, headers["X-Jobq-Timestamp"], raw_body, headers["X-Jobq-Signature"])
```

`verify` uses a constant-time comparison and rejects timestamps older than five minutes,
which blocks replayed requests.

## Why PostgreSQL instead of a broker

For most products the database is already there and already backed up. Keeping jobs in it
means enqueueing can be part of the same transaction as the business write, job state is
queryable with SQL, and there is one fewer system to run. The trade-off is throughput: a
dedicated broker wins at tens of thousands of jobs per second, which is past what this project
targets.

## Development

Requires Python 3.11+, [uv](https://docs.astral.sh/uv/) and PostgreSQL 16.

```bash
uv sync
createdb jobq_test
DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/jobq_test uv run pytest
uv run ruff check . && uv run ruff format --check .

# run locally
uv run alembic upgrade head
uv run uvicorn app.main:app --reload
uv run python -m app.worker
```

The test suite covers retries, backoff, dead-lettering, timeouts, crash recovery, concurrent
idempotent submits, signed webhook delivery and retry, and a race where eight workers drain
40 jobs and every job must run exactly once.

## Configuration

All settings come from environment variables; see [`.env.example`](.env.example) and
[`app/config.py`](app/config.py).
