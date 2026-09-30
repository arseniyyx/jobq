"""Worker process: `python -m app.worker`.

Runs N job loops, one webhook loop and one reaper loop in a single asyncio process.
SIGTERM/SIGINT stop claiming new work and let in-flight jobs finish.
"""

import asyncio
import logging
import os
import signal
import socket
import traceback

import httpx

from app.config import get_settings
from app.db import SessionLocal, engine
from app.handlers import REGISTRY, PermanentError
from app.queue import claim_job, complete_job, fail_job, requeue_stale
from app.webhooks import deliver_one

log = logging.getLogger("jobq.worker")


async def process_one(worker_id: str) -> bool:
    """Claim and run one job. Returns False when the queue had nothing ready."""
    settings = get_settings()
    async with SessionLocal() as session:
        job = await claim_job(session, worker_id)
        if job is None:
            return False
        log.info("job %s (%s) attempt %d/%d", job.id, job.type, job.attempts, job.max_attempts)

        handler = REGISTRY.get(job.type)
        if handler is None:
            await fail_job(session, job, f"no handler for type '{job.type}'", permanent=True)
            return True
        try:
            payload = handler.schema.model_validate(job.payload)
            result = await asyncio.wait_for(
                handler.func(payload), timeout=settings.job_timeout_seconds
            )
        except PermanentError as exc:
            await fail_job(session, job, str(exc), permanent=True)
        except TimeoutError:
            await fail_job(session, job, f"timed out after {settings.job_timeout_seconds}s")
        except Exception:
            await fail_job(session, job, traceback.format_exc(limit=5))
        else:
            await complete_job(session, job, result)
            log.info("job %s succeeded", job.id)
        return True


async def _loop(step, stop: asyncio.Event, idle: float) -> None:
    while not stop.is_set():
        try:
            did_work = await step()
        except Exception:
            log.exception("loop iteration failed")
            did_work = False
        if not did_work:
            try:
                await asyncio.wait_for(stop.wait(), timeout=idle)
            except TimeoutError:
                pass


async def run(stop: asyncio.Event | None = None) -> None:
    settings = get_settings()
    stop = stop or asyncio.Event()
    worker_id = f"{socket.gethostname()}:{os.getpid()}"

    async def reap() -> bool:
        async with SessionLocal() as session:
            if n := await requeue_stale(session):
                log.warning("requeued %d stale jobs", n)
        return False

    async with httpx.AsyncClient() as client:

        async def webhook_step() -> bool:
            async with SessionLocal() as session:
                return await deliver_one(session, client)

        tasks = [
            asyncio.create_task(
                _loop(
                    lambda i=i: process_one(f"{worker_id}#{i}"),
                    stop,
                    settings.poll_interval_seconds,
                )
            )
            for i in range(settings.worker_concurrency)
        ]
        tasks.append(asyncio.create_task(_loop(webhook_step, stop, settings.poll_interval_seconds)))
        tasks.append(asyncio.create_task(_loop(reap, stop, 30)))
        log.info("worker %s started with %d job loops", worker_id, settings.worker_concurrency)
        await asyncio.gather(*tasks)
    log.info("worker %s stopped", worker_id)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    async def _main() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        try:
            await run(stop)
        finally:
            await engine.dispose()

    asyncio.run(_main())


if __name__ == "__main__":
    main()
