"""gateway: the only public entry point. Routes /api/<service>/... by prefix and
tags every request with an x-request-id so it can be followed across services.

On Cloud Run every other service is private, so the gateway proves who it is
with an ID token on each call (ADR 0006). It also rate-limits writes, because
it's a public demo.
"""
import asyncio
import html
import os
import time
import uuid
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import FileResponse, HTMLResponse

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
    # Read-only tap on every topic, for the visualizer (ADR 0007).
    "feed": os.getenv("FEED_URL", "http://127.0.0.1:8087"),
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
# Per-IP request budgets for /api/*. In memory, so per instance: a speed bump
# that keeps a bot from running up Cloud Run, not a security boundary.
# Reads: the visualizer polls two endpoints every 0.6 s while an order runs
# (~200 GETs/min), so the read budget must sit comfortably above that.
WRITES_PER_MINUTE = int(os.getenv("WRITES_PER_MINUTE", "20"))
READS_PER_MINUTE = int(os.getenv("READS_PER_MINUTE", "300"))


class RateLimiter:
    """Per-key token bucket: `per_minute` tokens, refilled continuously, so a
    short burst is fine but a sustained flood is not. Two floats per client,
    and idle clients are forgotten, so many IPs can't fill the memory."""

    MAX_KEYS = 10_000

    def __init__(self, per_minute: int):
        self.capacity = float(per_minute)
        self.rate = per_minute / 60.0  # tokens per second
        self.buckets: dict[str, tuple[float, float]] = {}

    def allow(self, key: str, now: float | None = None) -> bool:
        now = time.monotonic() if now is None else now
        tokens, last = self.buckets.get(key, (self.capacity, now))
        tokens = min(self.capacity, tokens + (now - last) * self.rate)
        allowed = tokens >= 1
        self.buckets[key] = (tokens - 1 if allowed else tokens, now)
        if len(self.buckets) > self.MAX_KEYS:
            self._forget_idle(now)
        return allowed

    def retry_after(self, key: str) -> int:
        tokens, _ = self.buckets.get(key, (self.capacity, 0.0))
        return max(1, int((1 - tokens) / self.rate) + 1)

    def _forget_idle(self, now: float) -> None:
        # A bucket that would be full again has nothing worth remembering.
        full_after = self.capacity / self.rate
        self.buckets = {k: v for k, v in self.buckets.items() if now - v[1] < full_after}

# Tests swap this for an httpx.MockTransport.
transport: httpx.AsyncBaseTransport | None = None
_http: httpx.AsyncClient | None = None
writes = RateLimiter(WRITES_PER_MINUTE)
reads = RateLimiter(READS_PER_MINUTE)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # One client for all requests: creating one costs ~130 ms (CA bundle load).
    global _http
    _http = httpx.AsyncClient(timeout=TIMEOUT, transport=transport)
    yield
    await _http.aclose()


app = FastAPI(title="gateway", lifespan=lifespan)
telemetry.setup("gateway", app)


STATIC = os.path.join(os.path.dirname(__file__), "static")
# Same GA4 property as eonelabs.my and its other subdomains, so the demo shows
# up next to the rest of the site. A public ID, not a secret; unset = no GA.
GA_MEASUREMENT_ID = os.getenv("GA_MEASUREMENT_ID", "")


def _ga_snippet() -> str:
    """Standard gtag.js loader (same shape as eonelabs.my's), or ""."""
    if not GA_MEASUREMENT_ID:
        return ""
    ga_id = html.escape(GA_MEASUREMENT_ID, quote=True)
    return (
        f'<script async src="https://www.googletagmanager.com/gtag/js?id={ga_id}"></script>\n'
        "<script>\n"
        "  window.dataLayer = window.dataLayer || [];\n"
        "  function gtag(){dataLayer.push(arguments);}\n"
        "  gtag('js', new Date());\n"
        f"  gtag('config', '{ga_id}');\n"
        "</script>\n"
    )


@app.get("/health")
def health():
    return {"status": "ok"}


# Icons Google and browsers fetch from this host (it picks one favicon per
# hostname, so micro. needs its own rather than a cross-host link).
ICONS = {
    "/favicon.ico": ("favicon.ico", "image/x-icon"),
    "/icon-192.png": ("icon-192.png", "image/png"),
    "/apple-touch-icon.png": ("apple-touch-icon.png", "image/png"),
}


def _icon_route(path: str, filename: str, media_type: str) -> None:
    @app.get(path, include_in_schema=False)
    def icon():
        return FileResponse(os.path.join(STATIC, filename), media_type=media_type)


for _path, (_file, _type) in ICONS.items():
    _icon_route(_path, _file, _type)


@app.get("/", include_in_schema=False)
def visualizer():
    """The live order visualizer (ADR 0007), with GA4 when configured."""
    with open(os.path.join(STATIC, "index.html"), encoding="utf-8") as f:
        page = f.read()
    return HTMLResponse(page.replace("</head>", _ga_snippet() + "</head>", 1))


def _client_ip(request: Request) -> str:
    # Cloud Run puts the real client first in X-Forwarded-For.
    forwarded = request.headers.get("x-forwarded-for", "")
    return forwarded.split(",")[0].strip() or (request.client.host if request.client else "?")


async def _forward(request: Request, upstream: str, path: str) -> Response:
    request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
    headers = {h: request.headers[h] for h in FORWARD_HEADERS if h in request.headers}
    headers["x-request-id"] = request_id
    ip = _client_ip(request)
    limiter, kind = (reads, "requests") if request.method == "GET" else (writes, "orders")
    if not limiter.allow(ip):
        wait = limiter.retry_after(ip)
        return Response(f'{{"detail":"slow down: too many {kind}, try again in {wait}s"}}', 429,
                        media_type="application/json",
                        headers={"x-request-id": request_id, "retry-after": str(wait)})
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
