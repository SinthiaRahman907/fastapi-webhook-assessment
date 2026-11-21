"""
Background processing pipeline: preprocessing (CPU) -> delivery (I/O).

    HTTP handler                 Stage 1 (CPU)                   Stage 2 (I/O)
    ------------                 -------------                   -------------
    INSERT row (received)  -->   scrub_queue  --> ProcessPool -> delivery_queue --> httpx
    return 202                   (N workers)      scrub + hash   (M workers)        retry w/ backoff
                                                  UPDATE pending                    UPDATE delivered

* The request path only does a single indexed INSERT and returns; all CPU and
  network work happens after acknowledgment.
* The CPU-bound scrub runs in a ProcessPoolExecutor. The SHA-256 loop hashes
  32-byte inputs, for which CPython does NOT release the GIL, so a thread pool
  (asyncio.to_thread) would still starve the event loop.
* SQLite is the source of truth; the asyncio queues only carry row ids as
  wake-ups. On startup every unfinished row is re-enqueued, so an
  acknowledged event survives a process crash.
"""

from __future__ import annotations

import asyncio
import logging
import multiprocessing
import os
import random
from concurrent.futures import ProcessPoolExecutor

import httpx

from app.database import STATUS_PENDING, STATUS_RECEIVED, Database
from app.downstream import (
    PermanentDeliveryError,
    TransientDeliveryError,
    build_client,
    forward_event,
)
from app.processor import scrub_and_fingerprint

log = logging.getLogger("webhooks.pipeline")


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except ValueError:
        return default


def _die_with_parent() -> None:
    # If the API process is SIGKILLed it cannot shut the pool down; ask the
    # kernel to kill this worker when its parent dies so no orphans leak.
    try:
        import ctypes
        import signal

        PR_SET_PDEATHSIG = 1
        ctypes.CDLL("libc.so.6", use_errno=True).prctl(PR_SET_PDEATHSIG, signal.SIGKILL)
    except (OSError, AttributeError):
        pass  # non-Linux


def _init_worker() -> None:
    _die_with_parent()
    _lower_priority()


def _lower_priority() -> None:
    # Preprocessing is throughput work; the API process must win the CPU when
    # both are runnable so /health and acknowledgments stay fast. SCHED_IDLE
    # (Linux) means "only run on otherwise idle CPU"; the backlog is durable in
    # SQLite, so deferring it under contention is safe. Fallback: nice 19.
    try:
        os.sched_setscheduler(0, os.SCHED_IDLE, os.sched_param(0))
        return
    except (AttributeError, OSError):
        pass
    try:
        os.nice(19)
    except (AttributeError, OSError):
        pass


def _warmup() -> int:
    return os.getpid()


class Pipeline:
    def __init__(self, db: Database):
        self.db = db
        cpu = os.cpu_count() or 1
        # Leave at least one core for the event loop.
        self.cpu_workers = _env_int("SCRUB_WORKERS", max(1, cpu - 1))
        self.delivery_workers = _env_int("DELIVERY_CONCURRENCY", 16)
        # Retry budget is primarily time-based so that an outage of a known
        # length (e.g. 60 s) is always ridden out regardless of jitter luck.
        self.give_up_after = float(os.getenv("DELIVERY_GIVE_UP_SECONDS", "900"))
        self.max_attempts = _env_int("DELIVERY_MAX_ATTEMPTS", 100)
        self.base_backoff = float(os.getenv("DELIVERY_BASE_BACKOFF", "0.2"))
        self.max_backoff = float(os.getenv("DELIVERY_MAX_BACKOFF", "30"))
        self.lease_seconds = float(os.getenv("WORK_LEASE_SECONDS", "30"))
        # Hard upper bound on one delivery attempt. The lease held during an
        # attempt is always attempt_timeout + lease_seconds, so a slow attempt
        # can never outlive its lease and let a second worker deliver the same
        # row concurrently.
        self.attempt_timeout = float(os.getenv("DELIVERY_ATTEMPT_TIMEOUT", "10"))
        self.reap_interval = float(os.getenv("REAP_INTERVAL_SECONDS", "10"))

        self.scrub_queue: asyncio.Queue[int] = asyncio.Queue()
        self.delivery_queue: asyncio.Queue[int] = asyncio.Queue()
        self.executor: ProcessPoolExecutor | None = None
        self.client: httpx.AsyncClient | None = None
        self._tasks: list[asyncio.Task] = []

    # ---------------------------------------------------------------- lifecycle
    async def start(self) -> None:
        # "spawn" avoids forking a process that already owns threads
        # (the SQLite executor) and an event loop.
        self.executor = ProcessPoolExecutor(
            max_workers=self.cpu_workers,
            mp_context=multiprocessing.get_context("spawn"),
            initializer=_init_worker,
        )
        # Pay worker start-up cost before we report healthy, not on the first request.
        loop = asyncio.get_running_loop()
        await asyncio.gather(
            *(loop.run_in_executor(self.executor, _warmup) for _ in range(self.cpu_workers))
        )
        self.client = build_client()

        for i in range(self.cpu_workers):
            self._tasks.append(asyncio.create_task(self._scrub_worker(), name=f"scrub-{i}"))
        for i in range(self.delivery_workers):
            self._tasks.append(asyncio.create_task(self._delivery_worker(), name=f"deliver-{i}"))

        await self.recover()
        self._tasks.append(asyncio.create_task(self._reaper(), name="reaper"))

    async def _reaper(self) -> None:
        """Periodically re-enqueue rows whose lease expired (e.g. a stuck or
        crashed worker). Double-enqueueing is harmless: claim() is atomic."""
        while True:
            await asyncio.sleep(self.reap_interval)
            try:
                await self.recover()
            except Exception:
                log.exception("reaper pass failed")

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        if self.client is not None:
            await self.client.aclose()
        if self.executor is not None:
            # Wait (off the loop) for workers to exit so no orphaned
            # interpreter processes survive a SIGTERM / redeploy.
            executor, self.executor = self.executor, None
            await asyncio.to_thread(executor.shutdown, wait=True, cancel_futures=True)

    async def recover(self) -> None:
        """Re-enqueue work left unfinished by a previous run / crash."""
        for row_id, status in await self.db.recoverable():
            self.enqueue(row_id, status)

    def enqueue(self, row_id: int, status: str = STATUS_RECEIVED) -> None:
        if status == STATUS_RECEIVED:
            self.scrub_queue.put_nowait(row_id)
        elif status == STATUS_PENDING:
            self.delivery_queue.put_nowait(row_id)

    # ------------------------------------------------------------ stage 1: CPU
    async def _scrub_worker(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            row_id = await self.scrub_queue.get()
            try:
                row = await self.db.claim(row_id, STATUS_RECEIVED, self.lease_seconds)
                if row is None:  # already handled / leased elsewhere
                    continue
                cleaned, fingerprint = await loop.run_in_executor(
                    self.executor, scrub_and_fingerprint, row.raw_content or ""
                )
                await self.db.mark_scrubbed(row_id, cleaned, fingerprint)
                self.delivery_queue.put_nowait(row_id)
            except asyncio.CancelledError:
                raise
            except Exception:
                # Lease will expire and the reaper will retry it.
                log.exception("preprocessing failed for row %s", row_id)
            finally:
                self.scrub_queue.task_done()

    # ----------------------------------------------------------- stage 2: I/O
    async def _delivery_worker(self) -> None:
        while True:
            row_id = await self.delivery_queue.get()
            try:
                await self._deliver(row_id)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("delivery crashed for row %s", row_id)
            finally:
                self.delivery_queue.task_done()

    async def _deliver(self, row_id: int) -> None:
        row = await self.db.claim(row_id, STATUS_PENDING, self.lease_seconds)
        if row is None:
            return
        assert self.client is not None

        attempts = row.attempts
        loop_time = asyncio.get_running_loop().time
        first_try = loop_time()
        while True:
            attempts += 1
            # Renew the lease so it covers the worst-case duration of this attempt.
            await self.db.renew_lease(row_id, self.attempt_timeout + self.lease_seconds)
            try:
                try:
                    async with asyncio.timeout(self.attempt_timeout):
                        await forward_event(
                            self.client, row.event_id, row.cleaned_text, row.fingerprint, row.idempotency_key
                        )
                except TimeoutError as exc:
                    raise TransientDeliveryError(f"attempt exceeded {self.attempt_timeout}s") from exc
            except PermanentDeliveryError as exc:
                log.warning("permanent failure for %s: %s", row.event_id, exc)
                await self.db.mark_failed(row_id, attempts, str(exc))
                return
            except TransientDeliveryError as exc:
                if attempts >= self.max_attempts or loop_time() - first_try >= self.give_up_after:
                    log.error("giving up on %s after %d attempts: %s", row.event_id, attempts, exc)
                    await self.db.mark_failed(row_id, attempts, str(exc))
                    return
                # Exponential backoff with full jitter, so a recovering
                # downstream isn't hit by a synchronized retry stampede.
                delay = random.uniform(0, min(self.max_backoff, self.base_backoff * 2 ** (attempts - 1)))
                # Extend the lease past the sleep so nobody steals the row mid-retry.
                await self.db.record_attempt(row_id, attempts, str(exc), self.lease_seconds + delay)
                await asyncio.sleep(delay)
                continue

            await self.db.mark_delivered(row_id, attempts)
            return
