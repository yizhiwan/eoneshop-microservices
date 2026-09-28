import httpx
import pytest
from fastapi.testclient import TestClient

from app import main


def handler(request: httpx.Request) -> httpx.Response:
    if request.url.host == "down":
        raise httpx.ConnectError("down")
    return httpx.Response(200, json={"host": request.url.host, "path": request.url.path,
                                     "rid": request.headers["x-request-id"]})


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(main, "transport", httpx.MockTransport(handler))
    monkeypatch.setitem(main.ROUTES, "products", "http://catalog")
    monkeypatch.setitem(main.ROUTES, "orders", "http://order")
    return TestClient(main.app)


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
