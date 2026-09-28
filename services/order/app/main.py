"""order-svc: owns orders, and is the source of truth for how each one ends.

POST /orders records a PENDING order, emits order.created (via the outbox) and
answers 202 straight away. The final status arrives later as events from
catalog and payment. Clients poll GET /orders/{id}.

Saga (choreography, ADR 0004): every way an order can fail ends in ONE event,
order.cancelled. Catalog and payment undo their own step when they hear it.
A sweeper cancels orders stuck in PENDING, e.g. because a service is down and
its events were dead-lettered.
"""
import json
import os
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

from fastapi import Depends, FastAPI, Header, HTTPException, Response
from opentelemetry import trace
from pydantic import BaseModel, Field
from sqlalchemy import DateTime, Integer, String, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Mapped, Session, mapped_column

from shared.chaos import Chaos
from shared import telemetry
from shared.eventbus import EventBus

from .db import Base, SessionLocal, engine, get_db


class Order(Base):
    __tablename__ = "orders"
    id: Mapped[int] = mapped_column(primary_key=True)
    ref: Mapped[str] = mapped_column(String(64), unique=True)
    product_id: Mapped[int] = mapped_column(Integer)
    qty: Mapped[int] = mapped_column(Integer)
    total_cents: Mapped[int | None] = mapped_column(Integer, nullable=True)
    status: Mapped[str] = mapped_column(String(20))
    reason: Mapped[str | None] = mapped_column(String(40), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    # Trace context of the request that created the order, so work done later
    # without a request (the timeout sweeper) still joins the same trace.
    trace_ctx: Mapped[str] = mapped_column(String(200), default="{}")


ORDER_TIMEOUT_S = float(os.getenv("ORDER_TIMEOUT_S", "15"))

bus = EventBus(Base, SessionLocal, source="order")
chaos = Chaos()


def sweep_once(now: datetime | None = None) -> int:
    """Cancel orders that have been PENDING longer than ORDER_TIMEOUT_S."""
    cutoff = (now or datetime.now(timezone.utc)) - timedelta(seconds=ORDER_TIMEOUT_S)
    with SessionLocal() as db:
        stuck = db.scalars(select(Order).where(Order.status == "PENDING")).all()
        # SQLite hands back naive datetimes; treat them as UTC.
        stuck = [o for o in stuck if o.created_at.replace(tzinfo=timezone.utc) < cutoff]
        for order in stuck:
            with telemetry.tracer().start_as_current_span(
                "saga timeout", context=telemetry.context_from(json.loads(order.trace_ctx)),
                attributes={"order.ref": order.ref, "order.timeout_s": ORDER_TIMEOUT_S},
            ):
                _finish(db, order.ref, "CANCELLED", "timeout")
                telemetry.log("order timed out", severity="WARNING", order_ref=order.ref)
        db.commit()
        return len(stuck)


def start_sweeper(interval: float = 1.0) -> None:
    if os.getenv("SAGA_SWEEPER", "on") == "off":
        return

    def loop():
        while True:
            try:
                sweep_once()
            except Exception as e:
                telemetry.log("sweeper tick failed", severity="ERROR", error=str(e))
            time.sleep(interval)

    threading.Thread(target=loop, daemon=True, name="saga-sweeper").start()


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(engine)
    bus.start_relay()
    start_sweeper()
    yield


app = FastAPI(title="order-svc", lifespan=lifespan)
app.include_router(chaos.router)
telemetry.setup("order", app)


class OrderIn(BaseModel):
    product_id: int
    qty: int = Field(gt=0, le=10)


def _out(o: Order) -> dict:
    return {"id": o.id, "ref": o.ref, "product_id": o.product_id, "qty": o.qty,
            "total_cents": o.total_cents, "status": o.status, "reason": o.reason}


def _by_ref(db: Session, ref: str) -> Order | None:
    return db.scalar(select(Order).where(Order.ref == ref))


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/orders", status_code=202)
def place_order(body: OrderIn, response: Response, db: Session = Depends(get_db),
                idempotency_key: str | None = Header(None, max_length=64)):
    # A client retrying with the same Idempotency-Key gets the same order back
    # instead of a second one.
    if idempotency_key and (existing := _by_ref(db, idempotency_key)):
        response.status_code = 200
        return _out(existing)

    ref = idempotency_key or str(uuid.uuid4())
    trace.get_current_span().set_attribute("order.ref", ref)
    order = Order(ref=ref, product_id=body.product_id, qty=body.qty, status="PENDING",
                  trace_ctx=json.dumps(telemetry.current_carrier()))
    db.add(order)
    bus.add(db, "order.created", {"order_ref": ref, "product_id": body.product_id, "qty": body.qty})
    try:
        db.commit()
    except IntegrityError:  # two concurrent requests with the same key
        db.rollback()
        response.status_code = 200
        return _out(_by_ref(db, ref))
    return _out(order)


@app.get("/orders/{order_id}")
def get_order(order_id: int, db: Session = Depends(get_db)):
    order = db.get(Order, order_id)
    if order is None:
        raise HTTPException(404, "Order not found")
    return _out(order)


def _finish(db: Session, ref: str, status: str, reason: str | None = None, total: int | None = None) -> None:
    order = _by_ref(db, ref)
    if order is None or order.status != "PENDING":
        return  # unknown, or already final: events can arrive late or twice
    order.status, order.reason = status, reason
    if total is not None:
        order.total_cents = total
    trace.get_current_span().set_attribute("order.status", status)
    telemetry.log(f"order {status.lower()}", order_ref=ref, reason=reason)
    bus.add(db, f"order.{status.lower()}", {"order_ref": ref, "status": status, "reason": reason})


HANDLERS = {
    "stock.rejected": lambda db, d: _finish(db, d["order_ref"], "CANCELLED", d["reason"]),
    "payment.succeeded": lambda db, d: _finish(db, d["order_ref"], "COMPLETED", total=d["amount_cents"]),
    "payment.failed": lambda db, d: _finish(db, d["order_ref"], "CANCELLED", "payment_declined", d["amount_cents"]),
}


@app.post("/pubsub/push", status_code=204)
def push(envelope: dict, db: Session = Depends(get_db)):
    bus.handle_push(db, envelope, HANDLERS, before=chaos.disrupt)
