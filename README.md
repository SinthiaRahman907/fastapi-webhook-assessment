# Webhook Ingestion Service — Backend Technical Assessment

## Context
Our webhook ingestion service is dropping requests and timing out health checks during traffic spikes, and downstream partners are reporting duplicate events whenever their webhook sender retries after a slow response.

Your goal is to diagnose the root causes under realistic concurrent load and refactor the service so it handles traffic spikes reliably and idempotently.

## Setup
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -e ".[dev]"
```

## Running the Benchmark & Chaos Harness
The test suite starts both the FastAPI ingestion service and a mock flaky downstream receiver (`mock_downstream.py`), then simulates a 60-request burst containing duplicate retries while continuously probing `/health`:

```bash
pytest -v -s
```

*(Note: On the initial codebase, `pytest` is expected to fail.)*

## Acceptance Criteria
1. **Event-Loop Responsiveness:** Every `GET /health` probe sent while burst traffic is in flight must respond in **< 50ms**.
2. **Fast Ingestion Acknowledgment:** `POST /webhooks/ingest` must acknowledge valid requests (`200` or `202`) with **p95 latency < 250ms**.
3. **Strict Idempotency:** Concurrent duplicate deliveries sharing the same `X-Idempotency-Key` must never produce duplicate database rows or duplicate downstream deliveries.
4. **Resilient Downstream Delivery:** Transient `503` errors from the downstream service must be retried safely so all unique events reach `"delivered"` status.

## Constraints
- **No external infrastructure:** Keep the solution self-contained using Python and SQLite (do not add Redis, Celery, RabbitMQ, or PostgreSQL).
- Complete `DECISIONS.md` explaining your root-cause diagnosis and architectural choices.

---

## Solution Overview
See [`DECISIONS.md`](DECISIONS.md) for the full diagnosis and trade-offs.

```
POST /webhooks/ingest ──► INSERT … ON CONFLICT DO NOTHING (UNIQUE idempotency_key) ──► 202
                                   │ row id
                                   ▼
                 scrub workers ──► ProcessPoolExecutor (PII scrub + fingerprint) ──► status=pending
                                   │
                                   ▼
              delivery workers ──► shared httpx client, retry 5xx/timeouts w/ backoff+jitter ──► status=delivered
```

* `app/main.py`: thin request path, persists then acknowledges with `202`; duplicates get `202 {"duplicate": true}`, same key with a different body gets `409`.
* `app/database.py`: single SQLite connection on a dedicated thread, WAL, idempotency via `UNIQUE` constraint, lease-based work claiming.
* `app/pipeline.py`: CPU stage (process pool) and I/O stage (bounded async workers), crash recovery and lease reaper.
* `app/downstream.py`: pooled HTTP client; classifies transient vs. permanent failures.
* `diag.py`: replays the burst against an empty FastAPI app and the real service side by side, to show the machine's latency floor (`python diag.py`).
* `tests/test_resilience.py`: extra tests for cross-connection idempotency, `409` on key reuse, downstream outage recovery, hard-kill-after-ack durability, and no double delivery when an attempt outlasts its lease.

### Running the tests: what to expect
* **Python 3.11–3.14.** On Python 3.15, `pip install` currently fails because `pyyaml` (pulled in by `uvicorn[standard]`) has no pre-built 3.15 wheels yet and needs a C++ compiler.
* `pytest -v -s` runs 6 tests: the provided burst harness plus 5 resilience tests. All resilience tests pass on Linux and Windows (both verified).
* The harness's two **latency thresholds** (`/health` < 50 ms, ingest p95 < 250 ms) measure the whole machine, test client included. On Linux the ingest target passes comfortably (~55–110 ms p95); `/health` is borderline on a small 2-vCPU box because even an empty server sits near 50 ms there, and should have more headroom with more cores. On **Windows** even an *empty* FastAPI app misses them (~450 ms p95 measured), so a timing failure there says nothing about the service. Run `python diag.py` to see your machine's floor next to the service; see "Verification notes" in `DECISIONS.md`.

Tunables (env vars): `SCRUB_WORKERS`, `DELIVERY_CONCURRENCY`, `DELIVERY_BASE_BACKOFF`, `DELIVERY_MAX_BACKOFF`, `DELIVERY_GIVE_UP_SECONDS`, `DELIVERY_MAX_ATTEMPTS`, `DELIVERY_ATTEMPT_TIMEOUT`, `WORK_LEASE_SECONDS`, `REAP_INTERVAL_SECONDS`.
