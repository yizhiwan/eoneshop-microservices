"""order-svc: owns orders.

POST /orders now only records a PENDING order and emits order.created (via the
outbox) and answers 202 straight away. The final status arrives later as events
from catalog and payment. Clients poll GET /orders/{id}.
"""
import uuid
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Header, HTTPException, Response
from pydantic import BaseModel, Field
from sqlalchemy import Integer, String, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Mapped, Session, mapped_column

from shared.eventbus import EventBus, parse_push

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


bus = EventBus(Base, SessionLocal, source="order")


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(engine)
    bus.start_relay()
    yield


app = FastAPI(title="order-svc", lifespan=lifespan)


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
    order = Order(ref=ref, product_id=body.product_id, qty=body.qty, status="PENDING")
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
    bus.add(db, f"order.{status.lower()}", {"order_ref": ref, "status": status, "reason": reason})


HANDLERS = {
    "stock.rejected": lambda db, d: _finish(db, d["order_ref"], "CANCELLED", d["reason"]),
    "payment.succeeded": lambda db, d: _finish(db, d["order_ref"], "COMPLETED", total=d["amount_cents"]),
    # Known gap until Phase 4: the reserved stock is NOT released here.
    "payment.failed": lambda db, d: _finish(db, d["order_ref"], "CANCELLED", "payment_declined", d["amount_cents"]),
}


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
