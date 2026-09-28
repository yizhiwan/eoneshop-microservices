"""notification-svc: tells the customer how their order ended.

Pretends to send an email for order.completed / order.cancelled. Idempotency
matters most here: a duplicate event must not mean a duplicate email.
"""
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import Depends, FastAPI
from sqlalchemy import DateTime, String, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Mapped, Session, mapped_column

from shared.eventbus import EventBus, parse_push

from .db import Base, SessionLocal, engine, get_db


class Notification(Base):
    __tablename__ = "notifications"
    id: Mapped[int] = mapped_column(primary_key=True)
    order_ref: Mapped[str] = mapped_column(String(64))
    message: Mapped[str] = mapped_column(String(200))
    sent_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


bus = EventBus(Base, SessionLocal, source="notification")


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(engine)
    yield  # consume-only: nothing to relay


app = FastAPI(title="notification-svc", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/notifications")
def list_notifications(db: Session = Depends(get_db)):
    rows = db.scalars(select(Notification).order_by(Notification.id.desc()).limit(50))
    return [{"order_ref": n.order_ref, "message": n.message, "sent_at": n.sent_at.isoformat()} for n in rows]


def on_order_finished(db: Session, d: dict) -> None:
    if d["status"] == "COMPLETED":
        message = "Your order is confirmed. Thank you!"
    else:
        message = f"Sorry, your order was cancelled ({d['reason']})."
    db.add(Notification(order_ref=d["order_ref"], message=message))
    print(f"[notification] email for {d['order_ref']}: {message}")


HANDLERS = {"order.completed": on_order_finished, "order.cancelled": on_order_finished}


@app.post("/pubsub/push", status_code=204)
def push(envelope: dict, db: Session = Depends(get_db)):
    event = parse_push(envelope)
    handler = HANDLERS.get(event.type)
    if handler and bus.first_time(db, event):
        handler(db, event.data)
    try:
        db.commit()
    except IntegrityError:
        # A concurrent copy of the same event won the race to processed_events.
        db.rollback()
