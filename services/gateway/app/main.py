"""gateway: the only public entry point. Routes /api/<service>/... by prefix and
tags every request with an x-request-id so it can be followed across services."""
import os
import uuid
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request, Response

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
}
# /api/chaos/<name> -> that service's /chaos (fault injection, ADR 0004).
CHAOS_TARGETS = {"catalog": CATALOG_URL, "order": ORDER_URL,
                 "payment": PAYMENT_URL, "notification": NOTIFICATION_URL}
FORWARD_HEADERS = ("content-type", "idempotency-key")
# Must exceed the worst-case time of the slowest route behind it (see ADR 0002),
# otherwise the client gets a 503 for an order that actually completed.
TIMEOUT = httpx.Timeout(15.0, connect=1.0)

# Tests swap this for an httpx.MockTransport.
transport: httpx.AsyncBaseTransport | None = None
_http: httpx.AsyncClient | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    # One client for all requests: creating one costs ~130 ms (CA bundle load).
    global _http
    _http = httpx.AsyncClient(timeout=TIMEOUT, transport=transport)
    yield
    await _http.aclose()


app = FastAPI(title="gateway", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok"}


async def _forward(request: Request, url: str) -> Response:
    request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
    headers = {h: request.headers[h] for h in FORWARD_HEADERS if h in request.headers}
    headers["x-request-id"] = request_id
    try:
        r = await _http.request(
            request.method, url, content=await request.body(),
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
    if upstream is None:
        return Response(status_code=404)
    return await _forward(request, f"{upstream}/chaos")


@app.api_route("/api/{service}", methods=["GET", "POST"])
@app.api_route("/api/{service}/{path:path}", methods=["GET", "POST"])
async def proxy(service: str, request: Request, path: str = ""):
    upstream = ROUTES.get(service)
    if upstream is None:
        return Response(status_code=404)
    return await _forward(request, f"{upstream}/{service}" + (f"/{path}" if path else ""))
