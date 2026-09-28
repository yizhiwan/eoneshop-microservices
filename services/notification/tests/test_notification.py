import pytest
from fastapi.testclient import TestClient

from app.db import Base, engine
from app.main import app
from shared.eventbus import make_push


@pytest.fixture
def client():
    Base.metadata.drop_all(engine)
    with TestClient(app) as c:
        yield c


def test_cancelled_order_notifies_with_reason(client):
    client.post("/pubsub/push", json=make_push(
        "order.cancelled", {"order_ref": "r1", "status": "CANCELLED", "reason": "out_of_stock"}))
    [n] = client.get("/notifications").json()
    assert n["order_ref"] == "r1" and "out_of_stock" in n["message"]


def test_duplicate_event_sends_one_email(client):
    push = make_push("order.completed", {"order_ref": "r2", "status": "COMPLETED", "reason": None}, event_id="e-1")
    client.post("/pubsub/push", json=push)
    client.post("/pubsub/push", json=push)
    assert len(client.get("/notifications").json()) == 1
