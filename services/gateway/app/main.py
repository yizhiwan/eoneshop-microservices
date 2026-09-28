"""gateway: the only public entry point. Routes /api/<service>/... by prefix and
tags every request with an x-request-id so it can be followed across services.

On Cloud Run every other service is private, so the gateway proves who it is
with an ID token on each call (ADR 0006). It also rate-limits writes, because
it's a public demo.
"""
import asyncio
import os
import time
import uuid
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request, Response

from shared import gcp, telemetry

CATALOG_URL = os.getenv("CATALOG_URL", "http://127.0.0.1:8001")
ORDER_URL = os.getenv("ORDER_URL", "http://127.0.0.1:8002")
PAYMENT_URL = os.getenv("PAYMENT_URL", "http://127.0.0.1:8003")
NOTIFICATION_URL = os.getenv("NOTIFICATION_URL", "http://127.0.0.1:8004")

ROUTES = {
    "products": CATALOG_URL,
    "orders": ORDER_URL,
    "notifications": NOTIFICATION_URL,
    # Broker delivery log, read by the Phase 7 visualizer.
    "events": os.getenv("BROKER_URL", "http://127.0.0.1:8085"),
    # Local trace collector (ADR 0005).
    "traces": os.getenv("TRACES_URL", "http://127.0.0.1:8086"),
}
# /api/chaos/<name> -> that service's /chaos (fault injection, ADR 0004).
# Off in production until the Phase 7 visualizer puts guard rails on it.
CHAOS_ENABLED = os.getenv("CHAOS_ENABLED", "on") == "on"
CHAOS_TARGETS = {"catalog": CATALOG_URL, "order": ORDER_URL,
                 "payment": PAYMENT_URL, "notification": NOTIFICATION_URL}
FORWARD_HEADERS = ("content-type", "idempotency-key")
# Must exceed the worst-case time of the slowest route behind it (see ADR 0002),
# otherwise the client gets a 503 for an order that actually completed.
TIMEOUT = httpx.Timeout(15.0, connect=1.0)
# Writes per client IP per minute. In memory, so per instance: a speed bump
# for a public demo, not a security boundary.
WRITES_PER_MINUTE = int(os.getenv("WRITES_PER_MINUTE", "20"))

# Tests swap this for an httpx.MockTransport.
transport: httpx.AsyncBaseTransport | None = None
_http: httpx.AsyncClient | None = None
_writes: dict[str, list[float]] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    # One client for all requests: creating one costs ~130 ms (CA bundle load).
    global _http
    _http = httpx.AsyncClient(timeout=TIMEOUT, transport=transport)
    yield
    await _http.aclose()


app = FastAPI(title="gateway", lifespan=lifespan)
telemetry.setup("gateway", app)


@app.get("/health")
def health():
    return {"status": "ok"}


def _client_ip(request: Request) -> str:
    # Cloud Run puts the real client first in X-Forwarded-For.
    forwarded = request.headers.get("x-forwarded-for", "")
    return forwarded.split(",")[0].strip() or (request.client.host if request.client else "?")


def _allow_write(ip: str, now: float | None = None) -> bool:
    now = now or time.monotonic()
    recent = [t for t in _writes.get(ip, []) if now - t < 60]
    if len(recent) >= WRITES_PER_MINUTE:
        _writes[ip] = recent
        return False
    _writes[ip] = recent + [now]
    return True


async def _forward(request: Request, upstream: str, path: str) -> Response:
    request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
    headers = {h: request.headers[h] for h in FORWARD_HEADERS if h in request.headers}
    headers["x-request-id"] = request_id
    if request.method != "GET" and not _allow_write(_client_ip(request)):
        return Response('{"detail":"slow down: too many writes, try again in a minute"}', 429,
                        media_type="application/json", headers={"x-request-id": request_id})
    try:
        if gcp.on_cloud_run():
            # The audience is the target service's own URL; Cloud Run checks it.
            token = await asyncio.to_thread(gcp.id_token, upstream)
            headers["authorization"] = f"Bearer {token}"
        r = await _http.request(
            request.method, upstream + path, content=await request.body(),
            params=request.query_params, headers=headers,
        )
    except httpx.HTTPError:
        return Response('{"detail":"upstream unavailable"}', 503, media_type="application/json",
                        headers={"x-request-id": request_id})
    return Response(r.content, r.status_code, media_type=r.headers.get("content-type"),
                    headers={"x-request-id": request_id})


@app.api_route("/api/chaos/{target}", methods=["GET", "PUT"])
async def chaos(target: str, request: Request):
    upstream = CHAOS_TARGETS.get(target)
    if upstream is None or not CHAOS_ENABLED:
        return Response(status_code=404)
    return await _forward(request, upstream, "/chaos")


@app.api_route("/api/{service}", methods=["GET", "POST"])
@app.api_route("/api/{service}/{path:path}", methods=["GET", "POST"])
async def proxy(service: str, request: Request, path: str = ""):
    upstream = ROUTES.get(service)
    if upstream is None:
        return Response(status_code=404)
    return await _forward(request, upstream, f"/{service}" + (f"/{path}" if path else ""))
