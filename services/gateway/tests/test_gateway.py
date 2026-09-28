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
    monkeypatch.setattr(main, "_writes", {})
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
    monkeypatch.setattr(main, "WRITES_PER_MINUTE", 3)
    ip = {"x-forwarded-for": "203.0.113.7, 10.0.0.1"}
    codes = [client.post("/api/orders", json={}, headers=ip).status_code for _ in range(4)]
    assert codes == [200, 200, 200, 429]
    assert client.get("/api/orders/1", headers=ip).status_code == 200  # reads aren't limited
    other = {"x-forwarded-for": "198.51.100.9"}
    assert client.post("/api/orders", json={}, headers=other).status_code == 200


def test_chaos_can_be_switched_off(client, monkeypatch):
    monkeypatch.setattr(main, "CHAOS_ENABLED", False)
    assert client.get("/api/chaos/payment").status_code == 404
