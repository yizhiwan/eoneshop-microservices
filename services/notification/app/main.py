"""notification-svc: tells the customer how their order ended.

Pretends to send an email for order.completed / order.cancelled. Idempotency
matters most here: a duplicate event must not mean a duplicate email.
"""
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import Depends, FastAPI
from sqlalchemy import DateTime, String, select
from sqlalchemy.orm import Mapped, Session, mapped_column

from shared.chaos import Chaos
from shared import telemetry
from shared.eventbus import EventBus

from .db import Base, SessionLocal, engine, get_db


class Notification(Base):
    __tablename__ = "notifications"
    id: Mapped[int] = mapped_column(primary_key=True)
    order_ref: Mapped[str] = mapped_column(String(64))
    message: Mapped[str] = mapped_column(String(200))
    sent_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


# Stand-in for an email provider: things that can't be rolled back.
OUTBOX_OF_THE_WORLD: list[str] = []

bus = EventBus(Base, SessionLocal, source="notification")
chaos = Chaos()


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(engine)
    yield  # consume-only: nothing to relay


app = FastAPI(title="notification-svc", lifespan=lifespan)
app.include_router(chaos.router)
telemetry.setup("notification", app)


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
    OUTBOX_OF_THE_WORLD.append(d["order_ref"])
    telemetry.log("email sent", order_ref=d["order_ref"], text=message)


HANDLERS = {"order.completed": on_order_finished, "order.cancelled": on_order_finished}


@app.post("/pubsub/push", status_code=204)
def push(envelope: dict, db: Session = Depends(get_db)):
    bus.handle_push(db, envelope, HANDLERS, before=chaos.disrupt)
