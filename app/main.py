import hashlib
import json
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.database import Database
from app.pipeline import Pipeline


class WebhookPayload(BaseModel):
    event_id: str
    tenant_id: str
    content: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    db = Database()
    await db.connect()
    pipeline = Pipeline(db)
    await pipeline.start()
    app.state.db = db
    app.state.pipeline = pipeline
    try:
        yield
    finally:
        await pipeline.stop()
        await db.close()


app = FastAPI(title="Webhook Ingestion Service", lifespan=lifespan)


@app.get("/health")
async def health():
    # Deliberately trivial: it must never touch the DB or wait on the pipeline,
    # so it measures only event-loop responsiveness.
    return {"status": "ok"}


def _request_hash(payload: WebhookPayload) -> str:
    body = json.dumps(payload.model_dump(), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


@app.post("/webhooks/ingest", status_code=202)
async def ingest_webhook(
    payload: WebhookPayload,
    x_idempotency_key: str | None = Header(default=None),
):
    if not x_idempotency_key:
        raise HTTPException(status_code=400, detail="Missing X-Idempotency-Key header")

    db: Database = app.state.db
    pipeline: Pipeline = app.state.pipeline

    result = await db.insert_or_get(
        idempotency_key=x_idempotency_key,
        request_hash=_request_hash(payload),
        event_id=payload.event_id,
        tenant_id=payload.tenant_id,
        raw_content=payload.content,
    )

    if result.conflict:
        raise HTTPException(
            status_code=409,
            detail="X-Idempotency-Key was already used with a different request body",
        )

    if result.created:
        pipeline.enqueue(result.row_id)

    # Duplicates get the same 202 as the original: from the sender's point of
    # view a retry of an accepted webhook is simply "accepted".
    return JSONResponse(
        status_code=202,
        content={
            "status": "accepted",
            "event_id": payload.event_id,
            "duplicate": not result.created,
        },
    )
