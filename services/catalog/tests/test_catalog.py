import json

import pytest
from fastapi.testclient import TestClient

from app.db import Base, engine
from app.main import app, bus
from shared.eventbus import make_push


@pytest.fixture
def client():
    Base.metadata.drop_all(engine)
    with TestClient(app) as c:
        yield c


@pytest.fixture
def published():
    sent = []
    bus.sender = lambda topic, body: sent.append((topic, json.loads(body)["data"]))

    def flush():
        bus.relay_once()
        return sent
    return flush


def stock(client, pid):
    return next(p["stock"] for p in client.get("/products").json() if p["id"] == pid)


def order_created(ref, product_id, qty, event_id=None):
    return make_push("order.created", {"order_ref": ref, "product_id": product_id, "qty": qty}, event_id)


def test_reserves_stock_and_announces(client, published):
    assert client.post("/pubsub/push", json=order_created("r1", 2, 3)).status_code == 204
    assert stock(client, 2) == 7
    assert published() == [("stock.reserved", {"order_ref": "r1", "product_id": 2, "qty": 3, "amount_cents": 10500})]


def test_out_of_stock_rejected(client, published):
    client.post("/pubsub/push", json=order_created("r2", 3, 6))
    assert stock(client, 3) == 5
    assert published() == [("stock.rejected", {"order_ref": "r2", "reason": "out_of_stock"})]


def test_unknown_product_rejected(client, published):
    client.post("/pubsub/push", json=order_created("r3", 99, 1))
    assert published()[0][1]["reason"] == "unknown_product"


def test_duplicate_delivery_reserves_once(client, published):
    push = order_created("r4", 1, 2, event_id="same-event")
    client.post("/pubsub/push", json=push)
    client.post("/pubsub/push", json=push)
    assert stock(client, 1) == 18
    assert len(published()) == 1


def test_outbox_retries_when_broker_down(client):
    client.post("/pubsub/push", json=order_created("r5", 1, 1))

    def down(topic, body):
        raise ConnectionError("broker down")

    bus.sender = down
    assert bus.relay_once() == 0
    sent = []
    bus.sender = lambda topic, body: sent.append(topic)
    assert bus.relay_once() == 1 and sent == ["stock.reserved"]
