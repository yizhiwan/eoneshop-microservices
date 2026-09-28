"""feed: a read-only tap on every topic, for the live visualizer (ADR 0007).

It subscribes to all events (and the dead-letter topic) and keeps the most
recent ones in memory, indexed by order. The page polls
GET /feed?order=<ref>&after=<seq> and animates each event from the service
that published it to the services subscribed to it.

Nothing depends on it: if it's down or restarts, orders still work and only
the animation is missing. So in-memory storage and a single instance are fine.
"""
import json
import base64
from collections import OrderedDict, deque
from datetime import datetime, timezone
from itertools import count

from fastapi import FastAPI

from shared import telemetry

MAX_ORDERS = 500
MAX_EVENTS_PER_ORDER = 100

# Who receives what, so the page knows where to send each dot. Mirrors the
# subscriptions in scripts/gcp_setup.sh and scripts/dev.py.
SUBSCRIBERS = {
    "order.created": ["catalog"],
    "stock.reserved": ["payment"],
    "stock.rejected": ["order"],
    "payment.succeeded": ["order"],
    "payment.failed": ["order"],
    "order.completed": ["notification"],
    "order.cancelled": ["notification", "catalog", "payment"],
    "stock.released": [],
    "payment.refunded": [],
}

ORDERS: OrderedDict[str, deque] = OrderedDict()
_seen: set[str] = set()
_seq = count(1)

app = FastAPI(title="feed")
telemetry.setup("feed", app)


@app.get("/health")
def health():
    return {"status": "ok"}


def record(envelope: dict) -> dict | None:
    message = envelope["message"]
    body = json.loads(base64.b64decode(message["data"]))
    ref = body.get("data", {}).get("order_ref")
    if not ref:
        return None
    attrs = message.get("attributes") or {}
    # Pub/Sub marks messages it moved to the dead-letter topic.
    dead = "CloudPubSubDeadLetterSourceDeliveryCount" in attrs
    key = f"{body['event_id']}:{'dead' if dead else 'live'}"
    if key in _seen:
        return None  # at-least-once delivery: show each event once
    _seen.add(key)
    entry = {
        "seq": next(_seq),
        "at": datetime.now(timezone.utc).isoformat(),
        "occurred_at": body.get("occurred_at"),
        "event_id": body["event_id"],
        "type": body["type"],
        "source": body.get("source"),
        "to": [] if dead else SUBSCRIBERS.get(body["type"], []),
        "dead_letter": dead,
        "attempts": int(attrs.get("CloudPubSubDeadLetterSourceDeliveryCount", 0)) or None,
        # ".../subscriptions/eoneshop.order.created--catalog" -> "catalog"
        "failed_at": attrs.get("CloudPubSubDeadLetterSourceSubscription", "").rsplit("--", 1)[-1] or None,
        "data": {k: v for k, v in body.get("data", {}).items() if k != "order_ref"},
        "trace_id": (body.get("trace") or {}).get("traceparent", "--").split("-")[1] or None,
    }
    events = ORDERS.setdefault(ref, deque(maxlen=MAX_EVENTS_PER_ORDER))
    events.append(entry)
    ORDERS.move_to_end(ref)
    while len(ORDERS) > MAX_ORDERS:
        _, dropped = ORDERS.popitem(last=False)
        for e in dropped:
            _seen.discard(f"{e['event_id']}:{'dead' if e['dead_letter'] else 'live'}")
    return entry


@app.post("/pubsub/push", status_code=204)
def push(envelope: dict):
    record(envelope)


@app.get("/feed")
def feed(order: str, after: int = 0):
    return [e for e in ORDERS.get(order, ()) if e["seq"] > after]
