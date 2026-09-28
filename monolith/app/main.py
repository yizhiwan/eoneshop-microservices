from contextlib import asynccontextmanager

from fastapi import FastAPI

from . import catalog, orders
from .db import Base, SessionLocal, engine


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(engine)
    with SessionLocal() as db:
        catalog.seed(db)
    yield


app = FastAPI(title="EoneShop (monolith)", lifespan=lifespan)
app.include_router(catalog.router)
app.include_router(orders.router)


@app.get("/health")
def health():
    return {"status": "ok"}
