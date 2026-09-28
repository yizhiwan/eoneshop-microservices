"""payment-svc: fake payment provider. Stateless."""
import os

from fastapi import FastAPI
from pydantic import BaseModel, Field

# Demo "chaos" switch: amounts at or above this are declined.
FAIL_AT_CENTS = int(os.getenv("PAYMENT_FAIL_AT_CENTS", "10000"))

app = FastAPI(title="payment-svc")


class Charge(BaseModel):
    order_ref: str
    amount_cents: int = Field(gt=0)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/charges")
def charge(body: Charge):
    return {"order_ref": body.order_ref, "approved": body.amount_cents < FAIL_AT_CENTS}
