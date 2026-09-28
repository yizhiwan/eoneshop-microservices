"""Break things on purpose and check the saga puts everything back.

Start the stack first, with a short order timeout so the demo is quick:

    ORDER_TIMEOUT_S=4 python scripts/dev.py --dupes
    python scripts/saga_demo.py

Each scenario places an order through the gateway, injects a failure with the
chaos switch, waits for the order to settle, and checks that stock (and
payment) ended up consistent.
"""
import sys
import time

import httpx

API = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8080/api"
c = httpx.Client(base_url=API, timeout=20)


def stock() -> dict[int, int]:
    return {p["id"]: p["stock"] for p in c.get("/products").json()}


def chaos(target: str, **settings) -> None:
    c.put(f"/chaos/{target}", json=settings).raise_for_status()


def reset_chaos() -> None:
    for target in ("catalog", "order", "payment", "notification"):
        chaos(target, fail_rate=0, latency_ms=0, **({"decline_all": False} if target == "payment" else {}))


def settle(order_id: int, timeout: float = 30) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        order = c.get(f"/orders/{order_id}").json()
        if order["status"] != "PENDING":
            return order
        time.sleep(0.2)
    raise AssertionError(f"order {order_id} still PENDING after {timeout}s")


def wait_for(check, timeout: float = 30) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if check():
            return True
        time.sleep(0.2)
    return False


def topics_for(ref: str) -> list[str]:
    return [e["topic"] for e in c.get("/events").json() if e["order_ref"] == ref and e["result"] == "published"]


def scenario(name: str, product_id: int, qty: int, expect: str, before=None, during=None, after=None):
    reset_chaos()
    start = stock()
    if before:
        before()
    t0 = time.time()
    order = c.post("/orders", json={"product_id": product_id, "qty": qty}).json()
    if during:
        during()
    final = settle(order["id"])
    if after:
        after()
    ok_status = final["status"] == expect
    # Compensation runs after the order settles, so give it a moment to land.
    restored = expect == "COMPLETED" or wait_for(lambda: stock() == start)
    events = topics_for(order["ref"])
    print(f"\n== {name}")
    print(f"   order: {final['status']} ({final['reason'] or 'ok'}) in {time.time() - t0:.1f}s")
    print(f"   events: {' -> '.join(dict.fromkeys(events))}")
    print(f"   stock before {start} after {stock()}")
    assert ok_status, f"expected {expect}, got {final['status']}"
    assert restored, "stock was not restored"
    return events, order["ref"]


scenario("1. happy path", 1, 1, "COMPLETED")

ev, ref = scenario("2. payment declined -> stock released", 2, 3, "CANCELLED")
assert wait_for(lambda: "stock.released" in topics_for(ref))

scenario("3. catalog down (every push fails) -> timeout -> recovers later", 1, 2, "CANCELLED",
         before=lambda: chaos("catalog", fail_rate=1),
         after=lambda: chaos("catalog", fail_rate=0))

ev, ref = scenario("4. payment slower than the order timeout -> refund or void", 1, 2, "CANCELLED",
                   before=lambda: chaos("payment", latency_ms=6000))
# Either the charge went through and was refunded, or the cancel got there
# first and the late charge was blocked. Never charged-and-kept.
assert wait_for(lambda: "payment.succeeded" not in topics_for(ref) or "payment.refunded" in topics_for(ref))
print(f"   payment: {'refunded' if 'payment.refunded' in topics_for(ref) else 'never charged (tombstone)'}")

reset_chaos()
print("\nall scenarios passed")
