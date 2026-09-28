"""catalog-svc: products + inventory. Owns the products table; nobody else touches it.

Reacts to order.created by reserving stock, then announces stock.reserved or
stock.rejected. Saga compensation: on order.cancelled it releases the
reservation and announces stock.released.
"""
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI
from sqlalchemy import Integer, String
from sqlalchemy.orm import Mapped, Session, mapped_column

from shared.chaos import Chaos
from shared import telemetry
from shared.eventbus import EventBus

from .db import Base, SessionLocal, engine, get_db


class Product(Base):
    __tablename__ = "products"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(100))
    price_cents: Mapped[int] = mapped_column(Integer)
    stock: Mapped[int] = mapped_column(Integer)


SEED = [("Kopi O Beans", 2500, 20), ("Batik Mug", 3500, 10), ("Keropok Box", 1500, 5)]

bus = EventBus(Base, SessionLocal, source="catalog")
chaos = Chaos()


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(engine)
    with SessionLocal() as db:
        if db.query(Product).count() == 0:
            db.add_all(Product(name=n, price_cents=p, stock=s) for n, p, s in SEED)
            db.commit()
    bus.start_relay()
    yield


app = FastAPI(title="catalog-svc", lifespan=lifespan)
app.include_router(chaos.router)
telemetry.setup("catalog", app)


def _out(p: Product) -> dict:
    return {"id": p.id, "name": p.name, "price_cents": p.price_cents, "stock": p.stock}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/products")
def list_products(db: Session = Depends(get_db)):
    return [_out(p) for p in db.query(Product).order_by(Product.id)]


class Reservation(Base):
    """What catalog reserved for each order, so it can undo exactly that.
    Keyed by order_ref, so it doubles as business-key idempotency."""
    __tablename__ = "reservations"
    order_ref: Mapped[str] = mapped_column(String(64), primary_key=True)
    product_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    qty: Mapped[int] = mapped_column(Integer, default=0)
    # RESERVED | RELEASED | REJECTED | VOID (the cancel arrived before the order)
    status: Mapped[str] = mapped_column(String(10))


def on_order_created(db: Session, d: dict) -> None:
    if db.get(Reservation, d["order_ref"]) is not None:
        return  # already handled, or cancelled before it got here (VOID)
    product = db.get(Product, d["product_id"])
    if product is None or product.stock < d["qty"]:
        reason = "unknown_product" if product is None else "out_of_stock"
        db.add(Reservation(order_ref=d["order_ref"], status="REJECTED"))
        bus.add(db, "stock.rejected", {"order_ref": d["order_ref"], "reason": reason})
        return
    product.stock -= d["qty"]
    db.add(Reservation(order_ref=d["order_ref"], product_id=product.id, qty=d["qty"], status="RESERVED"))
    bus.add(db, "stock.reserved", {"order_ref": d["order_ref"], "product_id": product.id,
                                   "qty": d["qty"], "amount_cents": product.price_cents * d["qty"]})


def on_order_cancelled(db: Session, d: dict) -> None:
    """Compensation: give back whatever this order reserved."""
    reservation = db.get(Reservation, d["order_ref"])
    if reservation is None:
        # The cancel overtook order.created. Leave a tombstone so the late
        # order.created is ignored instead of holding stock forever.
        db.add(Reservation(order_ref=d["order_ref"], status="VOID"))
        return
    if reservation.status != "RESERVED":
        return
    db.get(Product, reservation.product_id).stock += reservation.qty
    reservation.status = "RELEASED"
    bus.add(db, "stock.released", {"order_ref": d["order_ref"], "product_id": reservation.product_id,
                                   "qty": reservation.qty})


HANDLERS = {"order.created": on_order_created, "order.cancelled": on_order_cancelled}


@app.post("/pubsub/push", status_code=204)
def push(envelope: dict, db: Session = Depends(get_db)):
    bus.handle_push(db, envelope, HANDLERS, before=chaos.disrupt)
