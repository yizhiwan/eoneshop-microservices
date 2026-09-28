"""order-svc tests, with catalog + payment faked via httpx.MockTransport."""
import json
import os

os.environ["DATABASE_URL"] = "sqlite:///./test_order.db"

import httpx
import pytest
from fastapi.testclient import TestClient

from app import clients
from app.db import Base, engine
from app.main import app


class FakeUpstreams:
    def __init__(self):
        self.stock, self.price = 10, 3500
        self.catalog_down = False
        self.released = 0

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        body = json.loads(request.content) if request.content else {}
        if path.endswith("/reserve"):
            if self.catalog_down:
                raise httpx.ConnectError("down")
            if body["qty"] > self.stock:
                return httpx.Response(409, json={"detail": "Insufficient stock"})
            self.stock -= body["qty"]
            return httpx.Response(200, json={"id": 1, "price_cents": self.price, "stock": self.stock})
        if path.endswith("/release"):
            self.stock += body["qty"]
            self.released += body["qty"]
            return httpx.Response(200, json={})
        if path == "/charges":
            return httpx.Response(200, json={"approved": body["amount_cents"] < 10000})
        return httpx.Response(404)


@pytest.fixture
def up():
    return FakeUpstreams()


@pytest.fixture
def client(up):
    Base.metadata.drop_all(engine)
    clients.transport = httpx.MockTransport(up.handler)
    with TestClient(app) as c:
        yield c
    clients.transport = None


def test_completed(client, up):
    r = client.post("/orders", json={"product_id": 1, "qty": 2})
    assert r.status_code == 201 and r.json()["status"] == "COMPLETED"
    assert up.stock == 8


def test_payment_declined_releases_stock(client, up):
    r = client.post("/orders", json={"product_id": 1, "qty": 3})  # 3 x 3500 = 10500
    assert r.json()["status"] == "CANCELLED"
    assert up.released == 3 and up.stock == 10


def test_out_of_stock(client, up):
    up.stock = 2
    assert client.post("/orders", json={"product_id": 1, "qty": 3}).status_code == 409


def test_catalog_down_returns_503(client, up):
    up.catalog_down = True
    assert client.post("/orders", json={"product_id": 1, "qty": 1}).status_code == 503
