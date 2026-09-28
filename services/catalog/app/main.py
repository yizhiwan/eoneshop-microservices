"""catalog-svc: products + inventory. Owns the products table; nobody else touches it."""
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import Integer, String
from sqlalchemy.orm import Mapped, Session, mapped_column

from .db import Base, SessionLocal, engine, get_db


class Product(Base):
    __tablename__ = "products"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(100))
    price_cents: Mapped[int] = mapped_column(Integer)
    stock: Mapped[int] = mapped_column(Integer)


SEED = [("Kopi O Beans", 2500, 20), ("Batik Mug", 3500, 10), ("Keropok Box", 1500, 5)]


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(engine)
    with SessionLocal() as db:
        if db.query(Product).count() == 0:
            db.add_all(Product(name=n, price_cents=p, stock=s) for n, p, s in SEED)
            db.commit()
    yield


app = FastAPI(title="catalog-svc", lifespan=lifespan)


class Qty(BaseModel):
    qty: int = Field(gt=0)


def _out(p: Product) -> dict:
    return {"id": p.id, "name": p.name, "price_cents": p.price_cents, "stock": p.stock}


def _get(db: Session, product_id: int) -> Product:
    product = db.get(Product, product_id)
    if product is None:
        raise HTTPException(404, "Product not found")
    return product


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/products")
def list_products(db: Session = Depends(get_db)):
    return [_out(p) for p in db.query(Product).order_by(Product.id)]


@app.post("/products/{product_id}/reserve")
def reserve(product_id: int, body: Qty, db: Session = Depends(get_db)):
    product = _get(db, product_id)
    if product.stock < body.qty:
        raise HTTPException(409, "Insufficient stock")
    product.stock -= body.qty
    db.commit()
    return _out(product)


@app.post("/products/{product_id}/release")
def release(product_id: int, body: Qty, db: Session = Depends(get_db)):
    product = _get(db, product_id)
    product.stock += body.qty
    db.commit()
    return _out(product)
