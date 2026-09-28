"""gateway: the only public entry point. Routes /api/<service>/... by prefix and
tags every request with an x-request-id so it can be followed across services."""
import os
import uuid

import httpx
from fastapi import FastAPI, Request, Response

ROUTES = {
    "products": os.getenv("CATALOG_URL", "http://localhost:8001"),
    "orders": os.getenv("ORDER_URL", "http://localhost:8002"),
}
# Must exceed the worst-case time of the slowest route behind it (see ADR 0002),
# otherwise the client gets a 503 for an order that actually completed.
TIMEOUT = httpx.Timeout(15.0, connect=1.0)

# Tests swap this for an httpx.MockTransport.
transport: httpx.AsyncBaseTransport | None = None

app = FastAPI(title="gateway")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.api_route("/api/{service}", methods=["GET", "POST"])
@app.api_route("/api/{service}/{path:path}", methods=["GET", "POST"])
async def proxy(service: str, request: Request, path: str = ""):
    upstream = ROUTES.get(service)
    if upstream is None:
        return Response(status_code=404)
    request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
    url = f"{upstream}/{service}" + (f"/{path}" if path else "")
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT, transport=transport) as c:
            r = await c.request(
                request.method, url, content=await request.body(),
                headers={"content-type": request.headers.get("content-type", "application/json"),
                         "x-request-id": request_id},
            )
    except httpx.HTTPError:
        return Response('{"detail":"upstream unavailable"}', 503, media_type="application/json",
                        headers={"x-request-id": request_id})
    return Response(r.content, r.status_code, media_type=r.headers.get("content-type"),
                    headers={"x-request-id": request_id})
