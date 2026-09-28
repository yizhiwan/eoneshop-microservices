"""catalog-svc: products + inventory. Owns the products table; nobody else touches it.

Reacts to order.created by reserving stock, then announces stock.reserved or
stock.rejected. It no longer exposes reserve/release over HTTP.
"""
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI
from sqlalchemy import Integer, String
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Mapped, Session, mapped_column

from shared.eventbus import EventBus, parse_push

from .db import Base, SessionLocal, engine, get_db


class Product(Base):
    __tablename__ = "products"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(100))
    price_cents: Mapped[int] = mapped_column(Integer)
    stock: Mapped[int] = mapped_column(Integer)


SEED = [("Kopi O Beans", 2500, 20), ("Batik Mug", 3500, 10), ("Keropok Box", 1500, 5)]

bus = EventBus(Base, SessionLocal, source="catalog")


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


def _out(p: Product) -> dict:
    return {"id": p.id, "name": p.name, "price_cents": p.price_cents, "stock": p.stock}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/products")
def list_products(db: Session = Depends(get_db)):
    return [_out(p) for p in db.query(Product).order_by(Product.id)]


def on_order_created(db: Session, d: dict) -> None:
    product = db.get(Product, d["product_id"])
    if product is None or product.stock < d["qty"]:
        reason = "unknown_product" if product is None else "out_of_stock"
        bus.add(db, "stock.rejected", {"order_ref": d["order_ref"], "reason": reason})
        return
    product.stock -= d["qty"]
    bus.add(db, "stock.reserved", {"order_ref": d["order_ref"], "product_id": product.id,
                                   "qty": d["qty"], "amount_cents": product.price_cents * d["qty"]})


HANDLERS = {"order.created": on_order_created}


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
