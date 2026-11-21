"""Downstream that takes longer to answer than the test's work lease (used by test_resilience)."""
import asyncio

from fastapi import FastAPI

app = FastAPI()
received: list[str] = []


@app.post("/events")
async def events(body: dict):
    await asyncio.sleep(2.5)
    received.append(body["event_id"])
    return {"accepted": True}


@app.get("/stats")
async def stats():
    return {"total_accepted": len(received), "unique_events": len(set(received))}
