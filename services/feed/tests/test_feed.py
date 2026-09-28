import pytest
from fastapi.testclient import TestClient

from app import main
from shared.eventbus import make_push


@pytest.fixture
def client():
    main.ORDERS.clear()
    main._seen.clear()
    return TestClient(main.app)


def push(client, type_, ref, event_id=None, **attrs):
    body = make_push(type_, {"order_ref": ref, "qty": 1}, event_id=event_id,
                     trace={"traceparent": f"00-{'ab' * 16}-{'cd' * 8}-01"})
    body["message"]["attributes"].update(attrs)
    assert client.post("/pubsub/push", json=body).status_code == 204


def test_events_per_order_with_routes(client):
    push(client, "order.created", "r1")
    push(client, "order.cancelled", "r1")
    push(client, "order.created", "other")
    events = client.get("/feed", params={"order": "r1"}).json()
    assert [e["type"] for e in events] == ["order.created", "order.cancelled"]
    assert events[0]["to"] == ["catalog"]
    assert events[1]["to"] == ["notification", "catalog", "payment"]
    assert events[0]["trace_id"] == "ab" * 16 and "order_ref" not in events[0]["data"]


def test_after_returns_only_new_events(client):
    push(client, "order.created", "r2")
    first = client.get("/feed", params={"order": "r2"}).json()
    push(client, "stock.reserved", "r2")
    newer = client.get("/feed", params={"order": "r2", "after": first[-1]["seq"]}).json()
    assert [e["type"] for e in newer] == ["stock.reserved"]


def test_duplicates_shown_once_dead_letters_marked(client):
    push(client, "order.created", "r3", event_id="e1")
    push(client, "order.created", "r3", event_id="e1")
    push(client, "order.created", "r3", event_id="e1", CloudPubSubDeadLetterSourceDeliveryCount="5",
         CloudPubSubDeadLetterSourceSubscription="projects/p/subscriptions/eoneshop.order.created--catalog")
    events = client.get("/feed", params={"order": "r3"}).json()
    assert [(e["dead_letter"], e["attempts"]) for e in events] == [(False, None), (True, 5)]
    assert events[1]["to"] == [] and events[1]["failed_at"] == "catalog"


def test_unknown_order_is_empty(client):
    assert client.get("/feed", params={"order": "nope"}).json() == []
