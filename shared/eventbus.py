"""Event plumbing shared by every service: envelope, transactional outbox,
idempotent-consumer bookkeeping, Pub/Sub push parsing and trace propagation.

Only transport plumbing lives here. Event payloads stay owned by the service
that publishes them (ADR 0003), so services don't couple through shared models.
"""
import base64
import json
import os
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable

import httpx
from opentelemetry.trace import SpanKind
from sqlalchemy import DateTime, String, Text, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Mapped, Session, mapped_column

from . import gcp, telemetry

# pubsub-lite locally; https://pubsub.googleapis.com in production (ADR 0006).
# Both speak the same publish API, so only the URL and auth differ.
PUBSUB_ENDPOINT = os.getenv("PUBSUB_ENDPOINT", os.getenv("BROKER_URL", "http://127.0.0.1:8085"))
PROJECT = os.getenv("PUBSUB_PROJECT", "eoneshop-local")
# Topics share a GCP project with other apps, so prod prefixes them ("eoneshop.").
TOPIC_PREFIX = os.getenv("TOPIC_PREFIX", "")
# On Cloud Run, CPU is throttled between requests, so a background thread can't
# be relied on to publish. RELAY_INLINE=on publishes right after each commit,
# while the request still has CPU; the thread and /internal/tick are the backup.
RELAY_INLINE = os.getenv("RELAY_INLINE", "off") == "on"


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class Event:
    event_id: str
    type: str
    source: str
    occurred_at: str
    data: dict
    # W3C trace context of whoever caused this event, e.g. {"traceparent": ...}
    trace: dict = field(default_factory=dict)


class EventBus:
    def __init__(self, Base, SessionLocal, source: str):
        self.SessionLocal = SessionLocal
        self.source = source
        # Tests replace this to capture messages instead of POSTing to the broker.
        self.sender = self._post_to_broker
        # Reused: creating an httpx client costs ~130 ms (it loads the CA bundle).
        self._http: httpx.Client | None = None

        class OutboxEvent(Base):
            __tablename__ = "outbox"
            id: Mapped[str] = mapped_column(String(36), primary_key=True)
            topic: Mapped[str] = mapped_column(String(100))
            body: Mapped[str] = mapped_column(Text)
            created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
            published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

        class ProcessedEvent(Base):
            __tablename__ = "processed_events"
            event_id: Mapped[str] = mapped_column(String(36), primary_key=True)
            processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)

        self.Outbox = OutboxEvent
        self.Processed = ProcessedEvent

    # --- publishing -------------------------------------------------------

    def add(self, db: Session, topic: str, data: dict) -> str:
        """Stage an event in the caller's transaction. It is only published after
        that transaction commits, so the state change and the event can't diverge.
        The current trace context is stored with it, so the trace survives the
        outbox and the broker."""
        event_id = str(uuid.uuid4())
        body = {"event_id": event_id, "type": topic, "source": self.source,
                "occurred_at": _now().isoformat(), "data": data,
                "trace": telemetry.current_carrier()}
        db.add(self.Outbox(id=event_id, topic=topic, body=json.dumps(body)))
        return event_id

    def relay_once(self) -> int:
        """Publish pending outbox rows in order. If the process dies between
        sending and marking a row, it is sent again later with the same
        event_id, and consumers drop the duplicate."""
        sent = 0
        with self.SessionLocal() as db:
            # Postgres: lock the rows we take and skip rows another relayer has
            # locked, so two instances never publish the same row. SQLite has
            # no row locks (SQLAlchemy drops the clause), and doesn't need them
            # with one writer at a time.
            rows = db.scalars(
                select(self.Outbox).where(self.Outbox.published_at.is_(None))
                .order_by(self.Outbox.created_at).limit(50)
                .with_for_update(skip_locked=True)
            ).all()
            for row in rows:
                body = json.loads(row.body)
                waited_ms = (_now() - row.created_at.replace(tzinfo=timezone.utc)).total_seconds() * 1000
                with telemetry.tracer().start_as_current_span(
                    f"publish {row.topic}", context=telemetry.context_from(body.get("trace")),
                    kind=SpanKind.PRODUCER,
                    attributes={"messaging.destination.name": row.topic, "event.id": row.id,
                                "outbox.wait_ms": round(waited_ms, 1),
                                "order.ref": body["data"].get("order_ref", "")},
                ) as span:
                    try:
                        self.sender(row.topic, row.body)
                    except Exception as e:
                        span.record_exception(e)
                        telemetry.log("outbox publish failed, will retry", severity="WARNING",
                                      topic=row.topic, error=str(e))
                        break
                row.published_at = _now()
                sent += 1
            db.commit()  # one commit, so the row locks are held until we're done
        return sent

    def relay_after_commit(self) -> None:
        """Call right after committing a transaction that staged events."""
        if RELAY_INLINE:
            try:
                self.relay_once()
            except Exception as e:  # the backup paths will retry
                telemetry.log("inline relay failed", severity="WARNING", error=str(e))

    def start_relay(self, interval: float = 0.2) -> None:
        if os.getenv("OUTBOX_RELAY", "on") == "off":
            return
        # Create the client up front, or the first publish pays ~130 ms for it.
        self._http = self._http or httpx.Client(timeout=3.0)

        def loop():
            while True:
                try:
                    self.relay_once()
                except Exception as e:
                    telemetry.log("relay tick failed", severity="ERROR", error=str(e))
                time.sleep(interval)

        threading.Thread(target=loop, daemon=True, name="outbox-relay").start()

    def _post_to_broker(self, topic: str, body: str) -> None:
        # Google Pub/Sub's topics.publish REST call (pubsub-lite copies it).
        url = f"{PUBSUB_ENDPOINT}/v1/projects/{PROJECT}/topics/{TOPIC_PREFIX}{topic}:publish"
        message = {"data": base64.b64encode(body.encode()).decode(), "attributes": {"type": topic}}
        headers = {}
        if PUBSUB_ENDPOINT.startswith("https://pubsub.googleapis.com"):
            headers["Authorization"] = f"Bearer {gcp.access_token()}"
        if self._http is None:
            self._http = httpx.Client(timeout=3.0)
        self._http.post(url, json={"messages": [message]}, headers=headers).raise_for_status()

    # --- consuming --------------------------------------------------------

    def first_time(self, db: Session, event: Event) -> bool:
        """Idempotent consumer: claim the event id in the caller's transaction
        BEFORE doing any work. False means someone already handled it
        (delivery is at-least-once).

        The flush writes the row now, taking the DB lock on that key. A
        concurrent copy of the same event blocks on it and then fails the
        unique constraint, so it never reaches the handler and can't repeat a
        side effect (an email, a log line) that the database can't roll back."""
        if db.get(self.Processed, event.event_id) is not None:
            return False
        db.add(self.Processed(event_id=event.event_id))
        try:
            db.flush()
        except IntegrityError:
            db.rollback()
            return False
        return True

    def handle_push(self, db: Session, envelope: dict, handlers: dict[str, Callable],
                    before: Callable[[], None] | None = None) -> None:
        """The whole push endpoint: decode, continue the producer's trace,
        dedupe, run the handler, commit. `before` runs inside the span (the
        chaos switch uses it), so injected failures show up in the trace."""
        event = parse_push(envelope)
        handler = handlers.get(event.type)
        with telemetry.tracer().start_as_current_span(
            f"process {event.type}", context=telemetry.context_from(event.trace),
            kind=SpanKind.CONSUMER,
            attributes={"messaging.destination.name": event.type, "event.id": event.event_id,
                        "order.ref": event.data.get("order_ref", "")},
        ) as span:
            if before:
                before()
            if handler is None:
                return
            if not self.first_time(db, event):
                span.set_attribute("event.duplicate", True)
                return
            handler(db, event.data)
            db.commit()
        self.relay_after_commit()


def parse_push(envelope: dict) -> Event:
    """Decode a Pub/Sub push request body into our event envelope."""
    return Event(**json.loads(base64.b64decode(envelope["message"]["data"])))


def make_push(type_: str, data: dict, event_id: str | None = None, trace: dict | None = None) -> dict:
    """Build a push body the way the broker would. Used by tests."""
    body = {"event_id": event_id or str(uuid.uuid4()), "type": type_, "source": "test",
            "occurred_at": _now().isoformat(), "data": data, "trace": trace or {}}
    return {"message": {"data": base64.b64encode(json.dumps(body).encode()).decode(),
                        "attributes": {"type": type_}, "messageId": "1"},
            "subscription": "projects/test/subscriptions/test"}
