"""Fake payment module. Becomes payment-svc in Phase 2."""
import os

# Demo "chaos" switch: amounts at or above this fail, to exercise rollback.
FAIL_AT_CENTS = int(os.getenv("PAYMENT_FAIL_AT_CENTS", "10000"))


def charge(amount_cents: int) -> bool:
    return amount_cents < FAIL_AT_CENTS
