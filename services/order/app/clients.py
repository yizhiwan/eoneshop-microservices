"""HTTP clients for the services order-svc depends on.

This is where a monolith function call becomes a network call: it can be slow,
fail, or succeed on the other side while we see an error. Hence explicit
timeouts, and retries on connection errors only (the request never arrived).
"""
import os

import httpx

CATALOG_URL = os.getenv("CATALOG_URL", "http://localhost:8001")
PAYMENT_URL = os.getenv("PAYMENT_URL", "http://localhost:8003")
TIMEOUT = httpx.Timeout(3.0, connect=1.0)

# Tests swap this for an httpx.MockTransport.
transport: httpx.BaseTransport | None = None


class UpstreamError(Exception):
    """A dependency is down or misbehaving (maps to 503)."""


class StockError(Exception):
    def __init__(self, status: int, detail: str):
        self.status, self.detail = status, detail


def _client() -> httpx.Client:
    return httpx.Client(timeout=TIMEOUT, transport=transport or httpx.HTTPTransport(retries=1))


def reserve_stock(product_id: int, qty: int) -> dict:
    try:
        with _client() as c:
            r = c.post(f"{CATALOG_URL}/products/{product_id}/reserve", json={"qty": qty})
    except httpx.HTTPError as e:
        raise UpstreamError(f"catalog unreachable: {e}") from e
    if r.status_code in (404, 409):
        raise StockError(r.status_code, r.json().get("detail", ""))
    if r.status_code != 200:
        raise UpstreamError(f"catalog returned {r.status_code}")
    return r.json()


def release_stock(product_id: int, qty: int) -> None:
    try:
        with _client() as c:
            c.post(f"{CATALOG_URL}/products/{product_id}/release", json={"qty": qty}).raise_for_status()
    except httpx.HTTPError as e:
        # Known gap: the stock stays reserved. Phases 3-4 fix this with events + a saga.
        print(f"[order] WARNING release failed for product {product_id}: {e}")


def charge(order_ref: str, amount_cents: int) -> bool:
    """Returns approval. Failing to reach payment is treated as declined."""
    try:
        with _client() as c:
            r = c.post(f"{PAYMENT_URL}/charges", json={"order_ref": order_ref, "amount_cents": amount_cents})
            r.raise_for_status()
            return bool(r.json()["approved"])
    except httpx.HTTPError as e:
        print(f"[order] payment unreachable, treating as declined: {e}")
        return False
