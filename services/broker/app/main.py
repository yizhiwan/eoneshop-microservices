"""pubsub-lite: a tiny stand-in for Google Pub/Sub, for local dev and learning.

It speaks the same publish REST shape and push envelope as real Pub/Sub, so the
services won't change when Phase 6 swaps in the real thing. The semantics are
copied on purpose: at-least-once delivery, retries with backoff, dead-lettering,
and no ordering guarantee. Set DUPLICATE_RATE=1 to deliver everything twice and
prove the consumers are idempotent.
"""
import asyncio
import base64
import json
import os
import random
import uuid
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from itertools import count

import httpx
from fastapi import FastAPI

PROJECT = os.getenv("PUBSUB_PROJECT", "eoneshop-local")
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "5"))
BACKOFF_S = float(os.getenv("BACKOFF_S", "0.5"))
DUPLICATE_RATE = float(os.getenv("DUPLICATE_RATE", "0"))
# Like Pub/Sub's dead-letter topic: push dead messages here, with the same
# CloudPubSubDeadLetter* attributes Pub/Sub adds (the visualizer's feed shows them).
DEAD_LETTER_URL = os.getenv("DEAD_LETTER_URL", "")

# Tests swap this for an httpx.MockTransport.
transport: httpx.AsyncBaseTransport | None = None


def parse_subscriptions(spec: str) -> dict[str, list[tuple[str, str]]]:
    """'topic=url[#name],...' -> {topic: [(subscription_name, url)]}

    The subscription is named '<topic>--<name>' like in production; without
    '#name' the name comes from the URL's host (and port)."""
    subs: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for entry in filter(None, (e.strip() for e in spec.split(","))):
        topic, target = entry.split("=", 1)
        url, _, name = target.partition("#")
        if not name:
            u = httpx.URL(url)
            name = u.host + (f"-{u.port}" if u.port else "")
        subs[topic].append((f"{topic}--{name}", url))
    return subs


SUBSCRIPTIONS = parse_subscriptions(os.getenv("SUBSCRIPTIONS", ""))
LOG: deque = deque(maxlen=1000)
DEAD_LETTERS: deque = deque(maxlen=500)
_seq = count(1)
_tasks: set[asyncio.Task] = set()
_http: httpx.AsyncClient | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    # One client for every delivery: creating one costs ~130 ms (CA bundle load).
    global _http
    _http = httpx.AsyncClient(timeout=10.0, transport=transport)
    yield
    await _http.aclose()


app = FastAPI(title="pubsub-lite", lifespan=lifespan)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _order_ref(message: dict) -> str | None:
    try:
        return json.loads(base64.b64decode(message["data"]))["data"].get("order_ref")
    except Exception:
        return None


def _record(message: dict, **fields) -> None:
    LOG.append({"seq": next(_seq), "at": _now(), "messageId": message["messageId"],
                "topic": message["attributes"].get("type"), "order_ref": _order_ref(message), **fields})


async def deliver(sub_name: str, url: str, message: dict) -> bool:
    envelope = {"message": message, "subscription": f"projects/{PROJECT}/subscriptions/{sub_name}"}
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            r = await _http.post(url, json=envelope)
            ok, result = r.status_code < 300, str(r.status_code)
        except httpx.HTTPError as e:
            ok, result = False, type(e).__name__
        _record(message, subscription=sub_name, attempt=attempt, result="ack" if ok else f"nack {result}")
        if ok:
            return True
        if attempt < MAX_ATTEMPTS:
            await asyncio.sleep(min(BACKOFF_S * 2 ** (attempt - 1), 10))
    DEAD_LETTERS.append({"subscription": sub_name, "message": message, "at": _now()})
    _record(message, subscription=sub_name, attempt=MAX_ATTEMPTS, result="dead-letter")
    if DEAD_LETTER_URL:
        dead = {**message, "attributes": {
            **message["attributes"],
            "CloudPubSubDeadLetterSourceDeliveryCount": str(MAX_ATTEMPTS),
            "CloudPubSubDeadLetterSourceSubscription": f"projects/{PROJECT}/subscriptions/{sub_name}",
        }}
        try:
            await _http.post(DEAD_LETTER_URL, json={"message": dead, "subscription": "dead-letter"})
        except httpx.HTTPError:
            pass  # best effort, like a DLQ nobody subscribed to
    return False


def _spawn(coro) -> None:
    task = asyncio.create_task(coro)
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/v1/projects/{project}/topics/{topic}:publish")
async def publish(project: str, topic: str, body: dict):
    ids = []
    for m in body.get("messages", []):
        message = {"data": m["data"], "attributes": m.get("attributes") or {"type": topic},
                   "messageId": uuid.uuid4().hex[:16], "publishTime": _now()}
        message["attributes"].setdefault("type", topic)
        ids.append(message["messageId"])
        _record(message, subscription=None, attempt=0, result="published")
        for name, url in SUBSCRIPTIONS.get(topic, []):
            copies = 2 if random.random() < DUPLICATE_RATE else 1
            for _ in range(copies):
                _spawn(deliver(name, url, message))
    return {"messageIds": ids}


@app.get("/events")
def events(after: int = 0):
    """Delivery log, oldest first. Poll with ?after=<last seq> (the visualizer will)."""
    return [e for e in LOG if e["seq"] > after]


@app.get("/dead-letters")
def dead_letters():
    return list(DEAD_LETTERS)
