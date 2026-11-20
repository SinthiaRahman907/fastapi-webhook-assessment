"""
Latency diagnostic: replays the test's 60-request burst against
  (1) an empty FastAPI app that does no work, and
  (2) the real service,
and prints p95 ingest latency and max /health latency for each.

Run from the project folder (venv active):  python diag.py
"""
import asyncio
import os
import subprocess
import sys
import tempfile
import time

from fastapi import FastAPI, Header
from pydantic import BaseModel

# ----------------------------------------------------------- the empty app
noop_app = FastAPI()


class _P(BaseModel):
    event_id: str
    tenant_id: str
    content: str


@noop_app.get("/health")
async def _health():
    return {"status": "ok"}


@noop_app.post("/webhooks/ingest", status_code=202)
async def _ingest(p: _P, x_idempotency_key: str | None = Header(default=None)):
    return {"status": "accepted"}


# ------------------------------------------------------------- the harness
APP_PORT, DS_PORT = 8951, 9051


async def wait_up(url):
    import httpx

    async with httpx.AsyncClient() as c:
        for _ in range(150):
            try:
                if (await c.get(url)).status_code < 500:
                    return
            except Exception:
                pass
            await asyncio.sleep(0.1)
    raise RuntimeError(f"{url} did not start")


async def burst(run_id: int):
    import httpx

    health, ingest = [], []
    stop = asyncio.Event()
    async with httpx.AsyncClient(timeout=15.0) as c:

        async def probe():
            while not stop.is_set():
                t = time.perf_counter()
                await c.get(f"http://127.0.0.1:{APP_PORT}/health")
                health.append((time.perf_counter() - t) * 1000)
                await asyncio.sleep(0.05)

        async def send(i):
            t = time.perf_counter()
            r = await c.post(
                f"http://127.0.0.1:{APP_PORT}/webhooks/ingest",
                json={"event_id": f"evt_{run_id}_{i // 2}", "tenant_id": "t", "content": f"user{i // 2}@example.com 123-45-6789"},
                headers={"X-Idempotency-Key": f"idem_{run_id}_{i // 2}"},
            )
            ingest.append((time.perf_counter() - t) * 1000)
            return r.status_code

        p = asyncio.create_task(probe())
        codes = await asyncio.gather(*(send(i) for i in range(60)))
        stop.set()
        await p
    ingest.sort()
    return ingest[int(len(ingest) * 0.95) - 1], max(health), all(c in (200, 202) for c in codes)


def run(label: str, module: str, runs: int = 3):
    env = os.environ.copy()
    env["WEBHOOK_DB_PATH"] = os.path.join(tempfile.mkdtemp(), "diag.db")
    env["DOWNSTREAM_URL"] = f"http://127.0.0.1:{DS_PORT}/events"
    uv = [sys.executable, "-m", "uvicorn", "--host", "127.0.0.1", "--log-level", "warning"]
    ds = subprocess.Popen(uv + ["mock_downstream:app", "--port", str(DS_PORT)], env=env)
    app = subprocess.Popen(uv + [module, "--port", str(APP_PORT)], env=env)
    try:
        asyncio.run(wait_up(f"http://127.0.0.1:{DS_PORT}/stats"))
        asyncio.run(wait_up(f"http://127.0.0.1:{APP_PORT}/health"))
        time.sleep(1.0)
        for i in range(runs):
            p95, hmax, ok = asyncio.run(burst(i))
            print(f"{label:<14} run {i + 1}:  ingest p95 = {p95:6.1f} ms   max /health = {hmax:6.1f} ms   all accepted = {ok}", flush=True)
            time.sleep(2.0)
    finally:
        app.terminate()
        ds.terminate()
        app.wait(timeout=10)
        ds.wait(timeout=10)


if __name__ == "__main__":
    print(f"Python {sys.version.split()[0]} on {sys.platform}, {os.cpu_count()} CPUs\n")
    run("EMPTY server", "diag:noop_app")
    print()
    run("REAL service", "app.main:app")
