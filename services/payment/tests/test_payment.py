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


def cancelled(ref):
    return make_push("order.cancelled", {"order_ref": ref, "status": "CANCELLED", "reason": "timeout"})


def test_cancel_after_charge_refunds(client, published):
    client.post("/pubsub/push", json=reserved("d", 5000))
    client.post("/pubsub/push", json=cancelled("d"))
    client.post("/pubsub/push", json=cancelled("d"))
    assert published() == [("payment.succeeded", {"order_ref": "d", "amount_cents": 5000}),
                           ("payment.refunded", {"order_ref": "d", "amount_cents": 5000})]


def test_cancel_before_charge_blocks_it(client, published):
    client.post("/pubsub/push", json=cancelled("e"))
    client.post("/pubsub/push", json=reserved("e", 5000))
    assert published() == []


def test_declined_charge_is_not_refunded(client, published):
    client.post("/pubsub/push", json=reserved("f", 20000))
    client.post("/pubsub/push", json=cancelled("f"))
    assert [t for t, _ in published()] == ["payment.failed"]


def test_decline_all_chaos(client, published):
    from app.main import chaos
    client.put("/chaos", json={"decline_all": True})
    try:
        client.post("/pubsub/push", json=reserved("g", 100))
    finally:
        chaos.reset()
    assert published()[0][0] == "payment.failed"


def reserved_with(ref, scenario):
    return make_push("stock.reserved", {"order_ref": ref, "product_id": 1, "qty": 1,
                                        "amount_cents": 100, "scenario": scenario})


def test_decline_payment_scenario(client, published):
    client.post("/pubsub/push", json=reserved_with("sp1", "decline_payment"))
    assert published()[0][0] == "payment.failed"


def test_slow_payment_scenario_loses_to_the_cancel(client, published, monkeypatch):
    """The cancel arrives while payment is still 'thinking': the tombstone wins."""
    import threading

    from app import main as payment_main
    monkeypatch.setattr(payment_main, "SLOW_PAYMENT_S", 0.5)
    slow = threading.Thread(target=lambda: client.post("/pubsub/push", json=reserved_with("sp2", "slow_payment")))
    slow.start()
    client.post("/pubsub/push", json=cancelled("sp2"))
    slow.join()
    assert published() == []
