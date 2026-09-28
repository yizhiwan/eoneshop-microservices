"""payment-svc: fake payment provider.

Reacts to stock.reserved by charging, then announces payment.succeeded or
payment.failed. Charges are keyed by order_ref, so an order is never charged
twice even if the same request arrives as two different events.
"""
import os
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI
from sqlalchemy import Boolean, Integer, String
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Mapped, Session, mapped_column

from shared.eventbus import EventBus, parse_push

from .db import Base, SessionLocal, engine, get_db

# Demo "chaos" switch: amounts at or above this are declined.
FAIL_AT_CENTS = int(os.getenv("PAYMENT_FAIL_AT_CENTS", "10000"))


class Charge(Base):
    __tablename__ = "charges"
    order_ref: Mapped[str] = mapped_column(String(64), primary_key=True)
    amount_cents: Mapped[int] = mapped_column(Integer)
    approved: Mapped[bool] = mapped_column(Boolean)


bus = EventBus(Base, SessionLocal, source="payment")


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(engine)
    bus.start_relay()
    yield


app = FastAPI(title="payment-svc", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok"}


def on_stock_reserved(db: Session, d: dict) -> None:
    if db.get(Charge, d["order_ref"]) is not None:
        return  # business-key idempotency: this order was already charged
    approved = d["amount_cents"] < FAIL_AT_CENTS
    db.add(Charge(order_ref=d["order_ref"], amount_cents=d["amount_cents"], approved=approved))
    bus.add(db, "payment.succeeded" if approved else "payment.failed",
            {"order_ref": d["order_ref"], "amount_cents": d["amount_cents"]})


HANDLERS = {"stock.reserved": on_stock_reserved}


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
