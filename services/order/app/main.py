"""order-svc: owns orders. Coordinates catalog + payment over synchronous HTTP.

Unlike the monolith there is no shared transaction: each step commits in its
own service, so a failure needs an explicit undo (release_stock). Phases 3-4
turn this into events + a saga.
"""
import uuid
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import Integer, String
from sqlalchemy.orm import Mapped, Session, mapped_column

from . import clients
from .db import Base, engine, get_db


class Order(Base):
    __tablename__ = "orders"
    id: Mapped[int] = mapped_column(primary_key=True)
    ref: Mapped[str] = mapped_column(String(36), unique=True)
    product_id: Mapped[int] = mapped_column(Integer)
    qty: Mapped[int] = mapped_column(Integer)
    total_cents: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(20))


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(engine)
    yield


app = FastAPI(title="order-svc", lifespan=lifespan)


class OrderIn(BaseModel):
    product_id: int
    qty: int = Field(gt=0, le=10)


def _out(o: Order) -> dict:
    return {"id": o.id, "ref": o.ref, "product_id": o.product_id, "qty": o.qty,
            "total_cents": o.total_cents, "status": o.status}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/orders", status_code=201)
def place_order(body: OrderIn, db: Session = Depends(get_db)):
    try:
        product = clients.reserve_stock(body.product_id, body.qty)
    except clients.StockError as e:
        raise HTTPException(e.status, e.detail)
    except clients.UpstreamError as e:
        raise HTTPException(503, str(e))

    ref = str(uuid.uuid4())
    total = product["price_cents"] * body.qty
    if clients.charge(ref, total):
        status = "COMPLETED"
    else:
        clients.release_stock(body.product_id, body.qty)
        status = "CANCELLED"

    order = Order(ref=ref, product_id=body.product_id, qty=body.qty, total_cents=total, status=status)
    db.add(order)
    db.commit()
    print(f"[notification] order {order.id} {status}")
    return _out(order)


@app.get("/orders/{order_id}")
def get_order(order_id: int, db: Session = Depends(get_db)):
    order = db.get(Order, order_id)
    if order is None:
        raise HTTPException(404, "Order not found")
    return _out(order)
