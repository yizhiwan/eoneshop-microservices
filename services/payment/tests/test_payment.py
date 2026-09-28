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


def reserved(ref, amount):
    return make_push("stock.reserved", {"order_ref": ref, "product_id": 1, "qty": 1, "amount_cents": amount})


def test_small_amount_succeeds(client, published):
    client.post("/pubsub/push", json=reserved("a", 5000))
    assert published() == [("payment.succeeded", {"order_ref": "a", "amount_cents": 5000})]


def test_large_amount_fails(client, published):
    client.post("/pubsub/push", json=reserved("b", 10500))
    assert published()[0][0] == "payment.failed"


def test_same_order_never_charged_twice(client, published):
    # Two *different* events (new event ids) for the same order_ref.
    client.post("/pubsub/push", json=reserved("c", 5000))
    client.post("/pubsub/push", json=reserved("c", 5000))
    assert len(published()) == 1
