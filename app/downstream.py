"""
Downstream delivery client.

A single shared httpx.AsyncClient (connection pooling + keep-alive) replaces
the original per-request client, which paid a TCP handshake per event and
leaked sockets under burst load.
"""

from __future__ import annotations

import os

import httpx

DOWNSTREAM_URL = os.getenv("DOWNSTREAM_URL", "http://127.0.0.1:9001/events")


class PermanentDeliveryError(Exception):
    """Downstream rejected the event in a way retrying will not fix (4xx)."""


class TransientDeliveryError(Exception):
    """Network failure, timeout, 429 or 5xx: safe and useful to retry."""


def build_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=httpx.Timeout(5.0, connect=2.0),
        limits=httpx.Limits(max_connections=32, max_keepalive_connections=16),
    )


async def forward_event(
    client: httpx.AsyncClient,
    event_id: str,
    cleaned_text: str,
    fingerprint: str,
    idempotency_key: str,
) -> dict:
    """
    POST one event downstream. Raises TransientDeliveryError or
    PermanentDeliveryError so the caller can decide whether to retry.

    The idempotency key is forwarded so a downstream that supports it can
    de-duplicate the rare case where it accepted a request but our side never
    saw the response (timeout after commit).
    """
    url = os.getenv("DOWNSTREAM_URL", DOWNSTREAM_URL)
    try:
        response = await client.post(
            url,
            json={"event_id": event_id, "text": cleaned_text, "fingerprint": fingerprint},
            headers={"Idempotency-Key": idempotency_key},
        )
    except httpx.TransportError as exc:  # connect errors, timeouts, resets
        raise TransientDeliveryError(f"{type(exc).__name__}: {exc}") from exc

    if response.status_code == 429 or response.status_code >= 500:
        raise TransientDeliveryError(f"HTTP {response.status_code}")
    if response.status_code >= 400:
        raise PermanentDeliveryError(f"HTTP {response.status_code}: {response.text[:200]}")
    return response.json()
