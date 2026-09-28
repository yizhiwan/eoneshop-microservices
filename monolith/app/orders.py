"""Order module. Becomes order-svc in Phase 2.

In the monolith, placing an order is ONE local DB transaction spanning
catalog, payment and orders. Phase 3-4 replace this with events + a saga.
"""
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import Integer, String
from sqlalchemy.orm import Mapped, Session, mapped_column

from . import catalog, payment
from .db import Base, get_db

router = APIRouter(prefix="/orders", tags=["orders"])


class Order(Base):
    __tablename__ = "orders"
    id: Mapped[int] = mapped_column(primary_key=True)
    product_id: Mapped[int] = mapped_column(Integer)
    qty: Mapped[int] = mapped_column(Integer)
    total_cents: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(20))


class OrderIn(BaseModel):
    product_id: int
    qty: int = Field(gt=0, le=10)


def _out(o: Order) -> dict:
    return {"id": o.id, "product_id": o.product_id, "qty": o.qty,
            "total_cents": o.total_cents, "status": o.status}


@router.post("", status_code=201)
def place_order(body: OrderIn, db: Session = Depends(get_db)):
    product = catalog.reserve_stock(db, body.product_id, body.qty)
    total = product.price_cents * body.qty
    if payment.charge(total):
        status = "COMPLETED"
    else:
        catalog.release_stock(db, body.product_id, body.qty)
        status = "CANCELLED"
    order = Order(product_id=body.product_id, qty=body.qty, total_cents=total, status=status)
    db.add(order)
    db.commit()
    print(f"[notification] order {order.id} {status}")
    return _out(order)


@router.get("/{order_id}")
def get_order(order_id: int, db: Session = Depends(get_db)):
    order = db.get(Order, order_id)
    if order is None:
        raise HTTPException(404, "Order not found")
    return _out(order)
