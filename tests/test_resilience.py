"""
Additional failure-mode tests (beyond the provided burst harness).

They back the answers in DECISIONS.md section 4:
  * idempotency holds across independent SQLite connections (i.e. processes)
  * reusing a key with a different body is rejected with 409
  * a downstream outage delays delivery but loses nothing
  * a hard crash (kill, no graceful shutdown) right after acknowledging loses nothing
  * a delivery attempt slower than the work lease is never delivered twice
"""

import asyncio
import os
import sqlite3
import subprocess
import sys
import time

import httpx

from app.database import Database

INGEST_PORT = 8911
DOWNSTREAM_PORT = 9011
INGEST_URL = f"http://127.0.0.1:{INGEST_PORT}"
DOWNSTREAM_URL = f"http://127.0.0.1:{DOWNSTREAM_PORT}"


def _env(db_file) -> dict:
    env = os.environ.copy()
    env.update(
        WEBHOOK_DB_PATH=str(db_file),
        DOWNSTREAM_URL=f"{DOWNSTREAM_URL}/events",
        DELIVERY_BASE_BACKOFF="0.05",
        DELIVERY_MAX_BACKOFF="0.5",
        DELIVERY_GIVE_UP_SECONDS="60",
        WORK_LEASE_SECONDS="1",
        DELIVERY_ATTEMPT_TIMEOUT="4",
        REAP_INTERVAL_SECONDS="0.3",
    )
    return env


def _start(module: str, port: int, env: dict) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-m", "uvicorn", module, "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
        env=env,
    )


def _stop(proc: subprocess.Popen) -> None:
    if proc.poll() is None:
        proc.terminate()
        proc.wait(timeout=10)


async def _wait_ok(url: str, timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    async with httpx.AsyncClient() as c:
        while time.monotonic() < deadline:
            try:
                if (await c.get(url)).status_code < 500:
                    return
            except httpx.TransportError:
                pass
            await asyncio.sleep(0.1)
    raise RuntimeError(f"{url} not up")


def _statuses(db_file) -> list[str]:
    conn = sqlite3.connect(db_file)
    try:
        return [r[0] for r in conn.execute("SELECT status FROM events ORDER BY id")]
    finally:
        conn.close()


async def _wait_all_delivered(db_file, n: int, timeout: float = 30.0) -> list[str]:
    deadline = time.monotonic() + timeout
    statuses: list[str] = []
    while time.monotonic() < deadline:
        statuses = _statuses(db_file)
        if len(statuses) == n and all(s == "delivered" for s in statuses):
            break
        await asyncio.sleep(0.2)
    return statuses


def _payload(i: int) -> dict:
    return {"event_id": f"evt_{i}", "tenant_id": "t", "content": f"user{i}@example.com 123-45-6789"}


async def test_idempotency_across_independent_connections(tmp_path):
    """Two Database objects = two connections/threads, as two processes would have."""
    path = str(tmp_path / "x.db")
    dbs = [Database(path) for _ in range(4)]
    for db in dbs:
        await db.connect()
    try:
        results = await asyncio.gather(
            *(db.insert_or_get("same-key", "h", "evt", "t", "raw") for db in dbs for _ in range(10))
        )
        assert sum(r.created for r in results) == 1
        assert len({r.row_id for r in results}) == 1
    finally:
        for db in dbs:
            await db.close()


async def test_key_reuse_with_different_body_is_409(tmp_path):
    db_file = tmp_path / "w.db"
    env = _env(db_file)
    ds, ing = _start("mock_downstream:app", DOWNSTREAM_PORT, env), _start("app.main:app", INGEST_PORT, env)
    try:
        await _wait_ok(f"{DOWNSTREAM_URL}/stats")
        await _wait_ok(f"{INGEST_URL}/health")
        async with httpx.AsyncClient() as c:
            h = {"X-Idempotency-Key": "k1"}
            assert (await c.post(f"{INGEST_URL}/webhooks/ingest", json=_payload(1), headers=h)).status_code == 202
            dup = await c.post(f"{INGEST_URL}/webhooks/ingest", json=_payload(1), headers=h)
            assert dup.status_code == 202 and dup.json()["duplicate"] is True
            bad = await c.post(f"{INGEST_URL}/webhooks/ingest", json=_payload(2), headers=h)
            assert bad.status_code == 409
            missing = await c.post(f"{INGEST_URL}/webhooks/ingest", json=_payload(3))
            assert missing.status_code == 400
    finally:
        _stop(ing)
        _stop(ds)


async def test_downstream_outage_then_recovery(tmp_path):
    """Downstream is down when events arrive; they are delivered once it comes back."""
    db_file = tmp_path / "o.db"
    env = _env(db_file)
    ing = _start("app.main:app", INGEST_PORT, env)
    ds = None
    try:
        await _wait_ok(f"{INGEST_URL}/health")
        async with httpx.AsyncClient() as c:
            for i in range(5):
                r = await c.post(f"{INGEST_URL}/webhooks/ingest", json=_payload(i), headers={"X-Idempotency-Key": f"o{i}"})
                assert r.status_code == 202
            await asyncio.sleep(2.0)  # downstream unreachable the whole time
            assert all(s == "pending" for s in _statuses(db_file))

            ds = _start("mock_downstream:app", DOWNSTREAM_PORT, env)
            await _wait_ok(f"{DOWNSTREAM_URL}/stats")
            statuses = await _wait_all_delivered(db_file, 5)
            assert statuses == ["delivered"] * 5
            stats = (await c.get(f"{DOWNSTREAM_URL}/stats")).json()
            assert stats["unique_events"] == stats["total_accepted"] == 5
    finally:
        _stop(ing)
        if ds:
            _stop(ds)


async def test_sigkill_after_ack_loses_nothing(tmp_path):
    """Acknowledged events survive a hard crash and are finished after restart."""
    db_file = tmp_path / "c.db"
    env = _env(db_file)
    # The hard kill below is intentional, so the killed server can't release its
    # multiprocessing semaphores; Python's resource tracker then cleans them up
    # and prints a "leaked semaphore" UserWarning. Silence that expected noise.
    env["PYTHONWARNINGS"] = "ignore:resource_tracker:UserWarning"
    ds = _start("mock_downstream:app", DOWNSTREAM_PORT, env)
    ing = _start("app.main:app", INGEST_PORT, env)
    try:
        await _wait_ok(f"{DOWNSTREAM_URL}/stats")
        await _wait_ok(f"{INGEST_URL}/health")
        async with httpx.AsyncClient() as c:
            await c.post(f"{DOWNSTREAM_URL}/reset")
            rs = await asyncio.gather(
                *(c.post(f"{INGEST_URL}/webhooks/ingest", json=_payload(i), headers={"X-Idempotency-Key": f"c{i}"}) for i in range(10))
            )
            assert all(r.status_code == 202 for r in rs)
            ing.kill()  # hard kill: SIGKILL on Linux/macOS, TerminateProcess on Windows
            ing.wait(timeout=5)
            assert len(_statuses(db_file)) == 10  # every acked event is durable

            ing = _start("app.main:app", INGEST_PORT, env)
            await _wait_ok(f"{INGEST_URL}/health")
            statuses = await _wait_all_delivered(db_file, 10)
            assert statuses == ["delivered"] * 10
            stats = (await c.get(f"{DOWNSTREAM_URL}/stats")).json()
            assert stats["unique_events"] == 10
    finally:
        _stop(ing)
        _stop(ds)


async def test_slow_attempt_longer_than_lease_is_not_delivered_twice(tmp_path):
    """
    Regression test: each downstream call takes 2.5 s while the work lease is
    1 s. Without per-attempt lease renewal, the reaper would hand the row to a
    second worker mid-attempt and the event would be delivered twice. (This is
    what happens on Windows, where connecting to a down port takes ~2 s.)
    """
    db_file = tmp_path / "s.db"
    env = _env(db_file)
    ds = _start("tests._slow_downstream:app", DOWNSTREAM_PORT, env)
    ing = _start("app.main:app", INGEST_PORT, env)
    try:
        await _wait_ok(f"{DOWNSTREAM_URL}/stats")
        await _wait_ok(f"{INGEST_URL}/health")
        async with httpx.AsyncClient() as c:
            for i in range(3):
                r = await c.post(f"{INGEST_URL}/webhooks/ingest", json=_payload(i), headers={"X-Idempotency-Key": f"s{i}"})
                assert r.status_code == 202
            statuses = await _wait_all_delivered(db_file, 3)
            assert statuses == ["delivered"] * 3
            await asyncio.sleep(3.0)  # give any duplicate attempt time to land
            stats = (await c.get(f"{DOWNSTREAM_URL}/stats")).json()
            assert stats["total_accepted"] == stats["unique_events"] == 3, stats
    finally:
        _stop(ing)
        _stop(ds)
