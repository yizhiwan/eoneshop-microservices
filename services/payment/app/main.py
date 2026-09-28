"""payment-svc: fake payment provider.

Reacts to stock.reserved by charging, then announces payment.succeeded or
payment.failed. Charges are keyed by order_ref, so an order is never charged
twice even if the same request arrives as two different events. Saga
compensation: on order.cancelled it refunds an approved charge.
"""
import os
import time
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI
from sqlalchemy import Integer, String
from sqlalchemy.orm import Mapped, Session, mapped_column

from shared.chaos import Chaos
from shared import telemetry
from shared.eventbus import EventBus

from .db import Base, SessionLocal, engine, get_db

# Demo "chaos" switch: amounts at or above this are declined.
FAIL_AT_CENTS = int(os.getenv("PAYMENT_FAIL_AT_CENTS", "10000"))
# "slow_payment" scenario: longer than the order timeout, so the order is
# cancelled while we're still thinking and the saga has to clean up.
SLOW_PAYMENT_S = float(os.getenv("SLOW_PAYMENT_S", "25"))


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
telemetry.setup("payment", app)


@app.get("/health")
def health():
    return {"status": "ok"}


def on_stock_reserved(db: Session, d: dict) -> None:
    scenario = d.get("scenario", "normal")
    if scenario == "slow_payment":
        # Commit the event claim first so we don't hold a transaction (and,
        # on SQLite, the whole database) open while we "think".
        db.commit()
        time.sleep(SLOW_PAYMENT_S)
    if db.get(Charge, d["order_ref"]) is not None:
        return  # already charged (or voided): never charge an order twice
    approved = (not chaos.settings["decline_all"] and scenario != "decline_payment"
                and d["amount_cents"] < FAIL_AT_CENTS)
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
    bus.handle_push(db, envelope, HANDLERS, before=chaos.disrupt)
