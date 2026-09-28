import base64
import json
import time

import httpx
import pytest
from fastapi.testclient import TestClient

from app import main


def _msg(order_ref="r1"):
    body = {"event_id": "e1", "type": "order.created", "data": {"order_ref": order_ref}}
    return {"data": base64.b64encode(json.dumps(body).encode()).decode(), "attributes": {"type": "order.created"}}


class Sub:
    """Fake subscriber that fails the first `failures` pushes."""

    def __init__(self, failures=0):
        self.failures, self.calls = failures, []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(json.loads(request.content))
        return httpx.Response(500 if len(self.calls) <= self.failures else 204)


@pytest.fixture
def setup(monkeypatch):
    def make(failures=0, max_attempts=5, duplicate_rate=0.0):
        sub = Sub(failures)
        monkeypatch.setattr(main, "transport", httpx.MockTransport(sub.handler))
        monkeypatch.setattr(main, "SUBSCRIPTIONS", main.parse_subscriptions("order.created=http://catalog/pubsub/push"))
        monkeypatch.setattr(main, "BACKOFF_S", 0.01)
        monkeypatch.setattr(main, "MAX_ATTEMPTS", max_attempts)
        monkeypatch.setattr(main, "DUPLICATE_RATE", duplicate_rate)
        main.LOG.clear()
        main.DEAD_LETTERS.clear()
        return sub
    return make


def publish_and_wait(client, until, timeout=3.0):
    r = client.post("/v1/projects/p/topics/order.created:publish", json={"messages": [_msg()]})
    assert r.status_code == 200 and len(r.json()["messageIds"]) == 1
    deadline = time.time() + timeout
    while time.time() < deadline and not until():
        time.sleep(0.02)


def test_parse_subscriptions():
    subs = main.parse_subscriptions("a=http://x:8080/p, a=http://y/p,b=http://z/p")
    assert subs["a"] == [("a--x", "http://x:8080/p"), ("a--y", "http://y/p")]
    assert subs["b"] == [("b--z", "http://z/p")]


def test_push_envelope_matches_pubsub(setup):
    sub = setup()
    with TestClient(main.app) as c:
        publish_and_wait(c, lambda: sub.calls)
    env = sub.calls[0]
    assert env["subscription"].endswith("/subscriptions/order.created--catalog")
    assert set(env["message"]) >= {"data", "attributes", "messageId", "publishTime"}


def test_retries_then_acks(setup):
    sub = setup(failures=2)
    with TestClient(main.app) as c:
        publish_and_wait(c, lambda: any(e["result"] == "ack" for e in main.LOG))
        results = [e["result"] for e in c.get("/events").json() if e["attempt"]]
    assert results == ["nack 500", "nack 500", "ack"]
    assert [e["order_ref"] for e in main.LOG][0] == "r1"


def test_dead_letter_after_max_attempts(setup):
    setup(failures=99, max_attempts=3)
    with TestClient(main.app) as c:
        publish_and_wait(c, lambda: main.DEAD_LETTERS)
        assert len(c.get("/dead-letters").json()) == 1


def test_duplicate_delivery(setup):
    sub = setup(duplicate_rate=1.0)
    with TestClient(main.app) as c:
        publish_and_wait(c, lambda: len(sub.calls) >= 2)
    assert len(sub.calls) == 2
