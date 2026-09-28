"""payment-svc: fake payment provider.

Reacts to stock.reserved by charging, then announces payment.succeeded or
payment.failed. Charges are keyed by order_ref, so an order is never charged
twice even if the same request arrives as two different events. Saga
compensation: on order.cancelled it refunds an approved charge.
"""
import os
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI
from sqlalchemy import Integer, String
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Mapped, Session, mapped_column

from shared.chaos import Chaos
from shared.eventbus import EventBus, parse_push

from .db import Base, SessionLocal, engine, get_db

# Demo "chaos" switch: amounts at or above this are declined.
FAIL_AT_CENTS = int(os.getenv("PAYMENT_FAIL_AT_CENTS", "10000"))


class Charge(Base):
    __tablename__ = "charges"
    order_ref: Mapped[str] = mapped_column(String(64), primary_key=True)
    amount_cents: Mapped[int] = mapped_column(Integer)
    # APPROVED | DECLINED | REFUNDED | VOID (the cancel arrived before the charge)
    status: Mapped[str] = mapped_column(String(10))


bus = EventBus(Base, SessionLocal, source="payment")
chaos = Chaos(decline_all=False)


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(engine)
    bus.start_relay()
    yield


app = FastAPI(title="payment-svc", lifespan=lifespan)
app.include_router(chaos.router)


@app.get("/health")
def health():
    return {"status": "ok"}


def on_stock_reserved(db: Session, d: dict) -> None:
    if db.get(Charge, d["order_ref"]) is not None:
        return  # already charged (or voided): never charge an order twice
    approved = not chaos.settings["decline_all"] and d["amount_cents"] < FAIL_AT_CENTS
    db.add(Charge(order_ref=d["order_ref"], amount_cents=d["amount_cents"],
                  status="APPROVED" if approved else "DECLINED"))
    bus.add(db, "payment.succeeded" if approved else "payment.failed",
            {"order_ref": d["order_ref"], "amount_cents": d["amount_cents"]})


def on_order_cancelled(db: Session, d: dict) -> None:
    """Compensation: refund if this order was charged, e.g. it timed out while
    payment was slow, and then the payment went through."""
    charge = db.get(Charge, d["order_ref"])
    if charge is None:
        db.add(Charge(order_ref=d["order_ref"], amount_cents=0, status="VOID"))  # tombstone
        return
    if charge.status != "APPROVED":
        return
    charge.status = "REFUNDED"
    bus.add(db, "payment.refunded", {"order_ref": d["order_ref"], "amount_cents": charge.amount_cents})


HANDLERS = {"stock.reserved": on_stock_reserved, "order.cancelled": on_order_cancelled}


@app.post("/pubsub/push", status_code=204)
def push(envelope: dict, db: Session = Depends(get_db)):
    chaos.disrupt()
    event = parse_push(envelope)
    handler = HANDLERS.get(event.type)
    if handler and bus.first_time(db, event):
        handler(db, event.data)
    try:
        db.commit()
    except IntegrityError:
        # A concurrent copy of the same event won the race to processed_events.
        db.rollback()
