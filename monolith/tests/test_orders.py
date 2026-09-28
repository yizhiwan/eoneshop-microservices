import os

os.environ["DATABASE_URL"] = "sqlite:///./test_shop.db"

import pytest
from fastapi.testclient import TestClient

from app.db import Base, engine
from app.main import app


@pytest.fixture
def client():
    Base.metadata.drop_all(engine)
    with TestClient(app) as c:
        yield c


def stock(client, pid):
    return next(p["stock"] for p in client.get("/products").json() if p["id"] == pid)


def test_order_completes_and_reserves_stock(client):
    r = client.post("/orders", json={"product_id": 1, "qty": 2})
    assert r.status_code == 201 and r.json()["status"] == "COMPLETED"
    assert stock(client, 1) == 18


def test_payment_failure_cancels_and_releases_stock(client):
    # Batik Mug 3500 x 3 = 10500 >= 10000 fail threshold
    r = client.post("/orders", json={"product_id": 2, "qty": 3})
    assert r.json()["status"] == "CANCELLED"
    assert stock(client, 2) == 10


def test_insufficient_stock(client):
    assert client.post("/orders", json={"product_id": 3, "qty": 6}).status_code == 409


def test_get_order(client):
    oid = client.post("/orders", json={"product_id": 1, "qty": 1}).json()["id"]
    assert client.get(f"/orders/{oid}").json()["status"] == "COMPLETED"
