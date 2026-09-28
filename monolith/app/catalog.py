"""Catalog + inventory module. Becomes catalog-svc in Phase 2."""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import Integer, String
from sqlalchemy.orm import Mapped, Session, mapped_column

from .db import Base, get_db

router = APIRouter(prefix="/products", tags=["catalog"])


class Product(Base):
    __tablename__ = "products"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(100))
    price_cents: Mapped[int] = mapped_column(Integer)
    stock: Mapped[int] = mapped_column(Integer)


SEED = [("Kopi O Beans", 2500, 20), ("Batik Mug", 3500, 10), ("Keropok Box", 1500, 5)]


def seed(db: Session) -> None:
    if db.query(Product).count() == 0:
        db.add_all(Product(name=n, price_cents=p, stock=s) for n, p, s in SEED)
        db.commit()


def reserve_stock(db: Session, product_id: int, qty: int) -> Product:
    product = db.get(Product, product_id)
    if product is None:
        raise HTTPException(404, "Product not found")
    if product.stock < qty:
        raise HTTPException(409, "Insufficient stock")
    product.stock -= qty
    return product


def release_stock(db: Session, product_id: int, qty: int) -> None:
    db.get(Product, product_id).stock += qty


@router.get("")
def list_products(db: Session = Depends(get_db)):
    return [
        {"id": p.id, "name": p.name, "price_cents": p.price_cents, "stock": p.stock}
        for p in db.query(Product).order_by(Product.id)
    ]
