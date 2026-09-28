"""Event plumbing shared by every service: envelope, transactional outbox,
idempotent-consumer bookkeeping and Pub/Sub push parsing.

Only transport plumbing lives here. Event payloads stay owned by the service
that publishes them (ADR 0003), so services don't couple through shared models.
"""
import base64
import json
import os
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

import httpx
from sqlalchemy import DateTime, String, Text, select
from sqlalchemy.orm import Mapped, Session, mapped_column

BROKER_URL = os.getenv("BROKER_URL", "http://127.0.0.1:8085")
PROJECT = os.getenv("PUBSUB_PROJECT", "eoneshop-local")


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class Event:
    event_id: str
    type: str
    source: str
    occurred_at: str
    data: dict


class EventBus:
    def __init__(self, Base, SessionLocal, source: str):
        self.SessionLocal = SessionLocal
        self.source = source
        # Tests replace this to capture messages instead of POSTing to the broker.
        self.sender = self._post_to_broker

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
        that transaction commits, so the state change and the event can't diverge."""
        event_id = str(uuid.uuid4())
        body = {"event_id": event_id, "type": topic, "source": self.source,
                "occurred_at": _now().isoformat(), "data": data}
        db.add(self.Outbox(id=event_id, topic=topic, body=json.dumps(body)))
        return event_id

    def relay_once(self) -> int:
        """Publish pending outbox rows in order. If the process dies between
        sending and marking a row, it is sent again later with the same
        event_id, and consumers drop the duplicate."""
        sent = 0
        with self.SessionLocal() as db:
            rows = db.scalars(
                select(self.Outbox).where(self.Outbox.published_at.is_(None))
                .order_by(self.Outbox.created_at).limit(50)
            ).all()
            for row in rows:
                try:
                    self.sender(row.topic, row.body)
                except Exception as e:
                    print(f"[{self.source}] outbox publish failed, will retry: {e}")
                    break
                row.published_at = _now()
                db.commit()
                sent += 1
        return sent

    def start_relay(self, interval: float = 0.2) -> None:
        if os.getenv("OUTBOX_RELAY", "on") == "off":
            return

        def loop():
            while True:
                try:
                    self.relay_once()
                except Exception as e:
                    print(f"[{self.source}] relay tick failed: {e}")
                time.sleep(interval)

        threading.Thread(target=loop, daemon=True, name="outbox-relay").start()

    def _post_to_broker(self, topic: str, body: str) -> None:
        # Same REST shape as Google Pub/Sub's topics.publish.
        url = f"{BROKER_URL}/v1/projects/{PROJECT}/topics/{topic}:publish"
        message = {"data": base64.b64encode(body.encode()).decode(), "attributes": {"type": topic}}
        httpx.post(url, json={"messages": [message]}, timeout=3.0).raise_for_status()

    # --- consuming --------------------------------------------------------

    def first_time(self, db: Session, event: Event) -> bool:
        """Idempotent consumer: record the event id in the caller's transaction.
        False means it was already handled (delivery is at-least-once)."""
        if db.get(self.Processed, event.event_id) is not None:
            return False
        db.add(self.Processed(event_id=event.event_id))
        return True


def parse_push(envelope: dict) -> Event:
    """Decode a Pub/Sub push request body into our event envelope."""
    return Event(**json.loads(base64.b64decode(envelope["message"]["data"])))


def make_push(type_: str, data: dict, event_id: str | None = None) -> dict:
    """Build a push body the way the broker would. Used by tests."""
    body = {"event_id": event_id or str(uuid.uuid4()), "type": type_, "source": "test",
            "occurred_at": _now().isoformat(), "data": data}
    return {"message": {"data": base64.b64encode(json.dumps(body).encode()).decode(),
                        "attributes": {"type": type_}, "messageId": "1"},
            "subscription": "projects/test/subscriptions/test"}
