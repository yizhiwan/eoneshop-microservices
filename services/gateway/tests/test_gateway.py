import httpx
import pytest
from fastapi.testclient import TestClient

from app import main


def handler(request: httpx.Request) -> httpx.Response:
    if request.url.host == "down":
        raise httpx.ConnectError("down")
    return httpx.Response(200, json={"host": request.url.host, "path": request.url.path,
                                     "query": str(request.url.query, "ascii"),
                                     "rid": request.headers["x-request-id"],
                                     "idem": request.headers.get("idempotency-key"),
                                     "auth": request.headers.get("authorization")})


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(main, "transport", httpx.MockTransport(handler))
    monkeypatch.setattr(main, "writes", main.RateLimiter(main.WRITES_PER_MINUTE))
    monkeypatch.setattr(main, "reads", main.RateLimiter(main.READS_PER_MINUTE))
    monkeypatch.setitem(main.ROUTES, "products", "http://catalog")
    monkeypatch.setitem(main.ROUTES, "orders", "http://order")
    with TestClient(main.app) as c:
        yield c


def test_routes_by_prefix(client):
    body = client.get("/api/products").json()
    assert body["host"] == "catalog" and body["path"] == "/products"
    assert client.get("/api/orders/5").json()["path"] == "/orders/5"


def test_request_id_propagates(client):
    r = client.get("/api/orders/1", headers={"x-request-id": "abc"})
    assert r.json()["rid"] == "abc" and r.headers["x-request-id"] == "abc"


def test_unknown_service_404(client):
    assert client.get("/api/nope").status_code == 404


def test_upstream_down_503(client, monkeypatch):
    monkeypatch.setitem(main.ROUTES, "orders", "http://down")
    assert client.get("/api/orders/1").status_code == 503


def test_forwards_idempotency_key_and_query(client):
    body = client.post("/api/orders", json={}, headers={"Idempotency-Key": "k1"}).json()
    assert body["idem"] == "k1"
    assert client.get("/api/events?after=5").json()["query"] == "after=5"


def test_chaos_routes_to_service(client, monkeypatch):
    monkeypatch.setitem(main.CHAOS_TARGETS, "payment", "http://payment")
    body = client.put("/api/chaos/payment", json={"fail_rate": 1}).json()
    assert body["host"] == "payment" and body["path"] == "/chaos"
    assert client.get("/api/chaos/nope").status_code == 404


def test_signs_upstream_calls_on_cloud_run(client, monkeypatch):
    monkeypatch.setattr(main.gcp, "on_cloud_run", lambda: True)
    audiences = []
    monkeypatch.setattr(main.gcp, "id_token", lambda aud: audiences.append(aud) or "tok")
    assert client.get("/api/orders/1").json()["auth"] == "Bearer tok"
    assert audiences == ["http://order"]  # audience = the target service's URL


def test_no_token_locally(client):
    assert client.get("/api/orders/1").json()["auth"] is None


def test_rate_limits_writes_per_ip(client, monkeypatch):
    monkeypatch.setattr(main, "writes", main.RateLimiter(3))
    ip = {"x-forwarded-for": "203.0.113.7, 10.0.0.1"}
    codes = [client.post("/api/orders", json={}, headers=ip).status_code for _ in range(4)]
    assert codes == [200, 200, 200, 429]
    assert client.get("/api/orders/1", headers=ip).status_code == 200  # reads aren't limited
    other = {"x-forwarded-for": "198.51.100.9"}
    assert client.post("/api/orders", json={}, headers=other).status_code == 200


def test_chaos_can_be_switched_off(client, monkeypatch):
    monkeypatch.setattr(main, "CHAOS_ENABLED", False)
    assert client.get("/api/chaos/payment").status_code == 404


def test_serves_visualizer_and_routes_feed(client):
    page = client.get("/")
    assert page.status_code == 200 and "Watch an order travel" in page.text
    body = client.get("/api/feed?order=r1&after=3").json()
    assert body["path"] == "/feed" and body["query"] == "order=r1&after=3"


def test_rate_limits_reads_per_ip_with_retry_after(client, monkeypatch):
    monkeypatch.setattr(main, "reads", main.RateLimiter(3))
    ip = {"x-forwarded-for": "203.0.113.8"}
    codes = [client.get("/api/orders/1", headers=ip).status_code for _ in range(4)]
    assert codes == [200, 200, 200, 429]
    blocked = client.get("/api/orders/1", headers=ip)
    assert blocked.status_code == 429 and int(blocked.headers["retry-after"]) >= 1
    # the page itself and health checks are never limited
    assert client.get("/", headers=ip).status_code == 200
    assert client.get("/health", headers=ip).status_code == 200


def test_serves_its_own_favicon(client):
    ico = client.get("/favicon.ico")
    assert ico.status_code == 200 and ico.headers["content-type"] == "image/x-icon"
    assert client.get("/apple-touch-icon.png").headers["content-type"] == "image/png"
    assert 'rel="icon" href="/favicon.ico"' in client.get("/").text


def test_default_read_budget_fits_the_visualizer(client):
    """The page polls /orders and /feed every 0.6 s: 200 GETs a minute."""
    limiter = main.RateLimiter(main.READS_PER_MINUTE)
    t, allowed = 0.0, 0
    while t < 60:
        allowed += limiter.allow("viewer", now=t) + limiter.allow("viewer", now=t)
        t += 0.6
    assert allowed == 200


def test_token_bucket_refills_and_forgets_idle_clients():
    limiter = main.RateLimiter(60)  # one token per second
    assert all(limiter.allow("a", now=0.0) for _ in range(60))
    assert not limiter.allow("a", now=0.0)
    assert limiter.allow("a", now=1.0)  # one second later: one new token
    limiter.MAX_KEYS = 2
    limiter.allow("b", now=100.0)
    limiter.allow("c", now=100.0)  # third key, "a" has been idle for 99 s
    assert "a" not in limiter.buckets


def test_page_has_ga4_only_when_configured(client, monkeypatch):
    monkeypatch.setattr(main, "GA_MEASUREMENT_ID", "")
    assert "googletagmanager" not in client.get("/").text
    monkeypatch.setattr(main, "GA_MEASUREMENT_ID", "G-TEST123")
    page = client.get("/").text
    assert "gtag/js?id=G-TEST123" in page and "gtag('config', 'G-TEST123')" in page
    assert page.index("googletagmanager") < page.index("</head>")
