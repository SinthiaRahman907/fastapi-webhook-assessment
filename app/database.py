"""
SQLite persistence layer.

Design notes
------------
* One long-lived sqlite3 connection per process, owned by a dedicated
  single-thread executor. Every public method runs a *whole* logical
  operation (statement(s) + fetch + commit) as one function on that thread,
  i.e. exactly one thread hop / event-loop wake-up per operation.

  - The original code opened a brand-new aiosqlite connection (and thread)
    three times per request, so a 60-request burst created ~180
    connections/threads that all fought over SQLite's single write lock.
  - Sharing one aiosqlite connection between coroutines is also unsafe:
    coroutine A can hold an open RETURNING cursor while coroutine B issues
    COMMIT ("cannot commit transaction - SQL statements in progress").
    Running each operation as a single function on a single thread makes
    operations atomic and serialized by construction, without asyncio locks,
    and with 1 hop instead of the 3-4 an aiosqlite execute/fetch/commit
    sequence costs.
* WAL journal mode + synchronous=NORMAL: readers never block the writer and
  commits are cheap appends.
* busy_timeout so an external reader/writer (the test opening the file, or a
  second worker process) waits instead of raising "database is locked".
* Idempotency is enforced by the database, not by application code: a
  UNIQUE constraint on idempotency_key plus INSERT ... ON CONFLICT DO NOTHING
  RETURNING id. Exactly one of N concurrent inserts gets a row back, and this
  holds across processes too.
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Callable, TypeVar

DB_PATH = os.getenv("WEBHOOK_DB_PATH", "webhooks.db")

# Status lifecycle:
#   received   -> request persisted + acknowledged, raw content awaiting scrubbing
#   pending    -> scrubbed + fingerprinted, raw content wiped, awaiting delivery
#   delivered  -> downstream accepted the event (terminal)
#   failed     -> permanent downstream rejection or retries exhausted (terminal)
STATUS_RECEIVED = "received"
STATUS_PENDING = "pending"
STATUS_DELIVERED = "delivered"
STATUS_FAILED = "failed"

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    idempotency_key  TEXT    NOT NULL UNIQUE,
    request_hash     TEXT    NOT NULL,
    event_id         TEXT    NOT NULL,
    tenant_id        TEXT    NOT NULL,
    raw_content      TEXT,
    cleaned_text     TEXT    NOT NULL DEFAULT '',
    fingerprint      TEXT    NOT NULL DEFAULT '',
    status           TEXT    NOT NULL DEFAULT 'received',
    attempts         INTEGER NOT NULL DEFAULT 0,
    last_error       TEXT,
    lease_until      REAL,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_events_status ON events(status);
"""

T = TypeVar("T")


@dataclass(slots=True)
class EventRow:
    id: int
    event_id: str
    tenant_id: str
    raw_content: str | None
    cleaned_text: str
    fingerprint: str
    status: str
    attempts: int
    idempotency_key: str


@dataclass(slots=True)
class InsertResult:
    row_id: int
    created: bool           # False => this was a duplicate delivery
    conflict: bool = False  # True  => same key reused with a different body


class Database:
    def __init__(self, path: str | None = None):
        self.path = path or os.getenv("WEBHOOK_DB_PATH", DB_PATH)
        self._conn: sqlite3.Connection | None = None
        # One thread == one connection == natural serialization of operations.
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="sqlite")

    async def _run(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, fn, self._conn)

    # ------------------------------------------------------------------ setup
    async def connect(self) -> None:
        def _open(_: Any) -> sqlite3.Connection:
            # isolation_level=None => autocommit: each statement is its own
            # transaction, so no explicit COMMIT round-trip is needed.
            conn = sqlite3.connect(self.path, timeout=5.0, check_same_thread=False, isolation_level=None)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=5000")
            conn.executescript(SCHEMA)
            return conn

        loop = asyncio.get_running_loop()
        self._conn = await loop.run_in_executor(self._executor, _open, None)

    async def close(self) -> None:
        if self._conn is not None:
            await self._run(lambda c: c.close())
            self._conn = None
        self._executor.shutdown(wait=True)

    # -------------------------------------------------------------- ingestion
    async def insert_or_get(
        self,
        idempotency_key: str,
        request_hash: str,
        event_id: str,
        tenant_id: str,
        raw_content: str,
    ) -> InsertResult:
        """
        Atomically claim an idempotency key. The UNIQUE constraint makes this
        race-free even across processes: SQLite serializes writers, and
        ON CONFLICT DO NOTHING means the loser of a race gets zero rows from
        RETURNING instead of an exception.
        """

        def op(c: sqlite3.Connection) -> InsertResult:
            row = c.execute(
                """
                INSERT INTO events (idempotency_key, request_hash, event_id, tenant_id, raw_content, status)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(idempotency_key) DO NOTHING
                RETURNING id
                """,
                (idempotency_key, request_hash, event_id, tenant_id, raw_content, STATUS_RECEIVED),
            ).fetchone()
            if row is not None:
                return InsertResult(row_id=row["id"], created=True)
            existing = c.execute(
                "SELECT id, request_hash FROM events WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            return InsertResult(
                row_id=existing["id"],
                created=False,
                conflict=existing["request_hash"] != request_hash,
            )

        return await self._run(op)

    # ------------------------------------------------------------- the worker
    async def claim(self, row_id: int, expected_status: str, lease_seconds: float) -> EventRow | None:
        """
        Take a time-bounded lease on a row in `expected_status`.

        The conditional UPDATE is the mutual-exclusion primitive: only one
        caller (task or process) can move lease_until forward, so a row is
        never processed twice concurrently. If the holder crashes, the lease
        expires and the reaper picks the row up again.
        """

        def op(c: sqlite3.Connection) -> EventRow | None:
            now = time.time()
            row = c.execute(
                """
                UPDATE events
                   SET lease_until = ?, updated_at = CURRENT_TIMESTAMP
                 WHERE id = ? AND status = ? AND (lease_until IS NULL OR lease_until < ?)
                RETURNING id, event_id, tenant_id, raw_content, cleaned_text, fingerprint,
                          status, attempts, idempotency_key
                """,
                (now + lease_seconds, row_id, expected_status, now),
            ).fetchone()
            return EventRow(**dict(row)) if row is not None else None

        return await self._run(op)

    async def mark_scrubbed(self, row_id: int, cleaned_text: str, fingerprint: str) -> None:
        # Raw (PII-bearing) content is wiped in the same statement that stores
        # the redacted text, so it never outlives preprocessing.
        await self._run(
            lambda c: c.execute(
                """
                UPDATE events
                   SET cleaned_text = ?, fingerprint = ?, raw_content = NULL,
                       status = ?, lease_until = NULL, updated_at = CURRENT_TIMESTAMP
                 WHERE id = ? AND status = ?
                """,
                (cleaned_text, fingerprint, STATUS_PENDING, row_id, STATUS_RECEIVED),
            )
        )

    async def mark_delivered(self, row_id: int, attempts: int) -> None:
        await self._run(
            lambda c: c.execute(
                """
                UPDATE events
                   SET status = ?, attempts = ?, last_error = NULL, lease_until = NULL,
                       updated_at = CURRENT_TIMESTAMP
                 WHERE id = ?
                """,
                (STATUS_DELIVERED, attempts, row_id),
            )
        )

    async def record_attempt(self, row_id: int, attempts: int, error: str, lease_seconds: float) -> None:
        await self._run(
            lambda c: c.execute(
                """
                UPDATE events
                   SET attempts = ?, last_error = ?, lease_until = ?, updated_at = CURRENT_TIMESTAMP
                 WHERE id = ?
                """,
                (attempts, error[:500], time.time() + lease_seconds, row_id),
            )
        )

    async def mark_failed(self, row_id: int, attempts: int, error: str) -> None:
        await self._run(
            lambda c: c.execute(
                """
                UPDATE events
                   SET status = ?, attempts = ?, last_error = ?, lease_until = NULL,
                       updated_at = CURRENT_TIMESTAMP
                 WHERE id = ?
                """,
                (STATUS_FAILED, attempts, error[:500], row_id),
            )
        )

    async def recoverable(self) -> list[tuple[int, str]]:
        """Rows left unfinished (e.g. by a crash) whose lease has expired."""

        def op(c: sqlite3.Connection) -> list[tuple[int, str]]:
            rows = c.execute(
                """
                SELECT id, status FROM events
                 WHERE status IN (?, ?) AND (lease_until IS NULL OR lease_until < ?)
                 ORDER BY id
                """,
                (STATUS_RECEIVED, STATUS_PENDING, time.time()),
            ).fetchall()
            return [(r["id"], r["status"]) for r in rows]

        return await self._run(op)
