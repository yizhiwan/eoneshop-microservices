# ADR 0004 — Saga: one cancel event, local compensations, chaos switch

**Status:** accepted (2026-09-28)

## Context
Phase 3 left a gap: when payment was declined, the order was cancelled but the
reserved stock was never given back. More generally, with no shared transaction,
any step can fail after earlier steps have already committed in other services.
There are also failures nobody reports at all: a service that is down never
answers, its events get dead-lettered, and the order would sit in PENDING forever.

## Decisions
1. **Choreography, not an orchestrator.** No central coordinator. Each service
   undoes its own step. That fits a flow this small; an orchestrator (a service
   that tells each step what to do) becomes worth it once flows branch a lot.
2. **One trigger for every compensation: `order.cancelled`.** order-svc owns the
   order, so it alone decides that an order is dead, whatever the reason
   (`out_of_stock`, `payment_declined`, `timeout`). Catalog and payment each
   react to that single event:
   - catalog: release the reservation → `stock.released`
   - payment: refund an approved charge → `payment.refunded`

   Each service remembers what it did per order (`reservations`, `charges`), so
   it undoes exactly that and never trusts another service's numbers.
3. **Timeouts are failures too.** A sweeper in order-svc cancels orders PENDING
   longer than `ORDER_TIMEOUT_S` (default 15 s). A late `payment.succeeded` for
   a cancelled order is ignored by order-svc and refunded by payment.
4. **Tombstones for events that arrive out of order.** If `order.cancelled`
   reaches a service before the step it cancels (e.g. catalog is down, the order
   times out, then catalog recovers), the service stores a `VOID` record. The
   late `order.created` / `stock.reserved` then finds it and does nothing,
   instead of reserving stock or charging for an order that's already dead.
5. **Chaos switch** (`shared/chaos.py`): every consumer exposes `GET/PUT /chaos`
   (`fail_rate`, `latency_ms`, payment also `decline_all`), reachable via
   `/api/chaos/<service>` on the gateway. `scripts/saga_demo.py` uses it to run
   four scenarios and assert stock always ends where it started.

## Findings
- **All four scenarios pass with every message delivered twice:**

  | Scenario | Result |
  |---|---|
  | happy path | COMPLETED |
  | payment declined | CANCELLED, stock released |
  | catalog failing every push | CANCELLED by timeout, stock untouched |
  | payment slower than the timeout | CANCELLED, charge blocked by a tombstone (or refunded) |

- **Creating an httpx client costs ~130 ms** (it loads the CA bundle). The
  broker made a new one per delivery, the outbox per publish, and the gateway
  per request. That was about 270 ms per hop, so ~2.5 s per order. Reusing one
  long-lived client per process: happy path **2.6 s → 0.6 s**, declined order
  **4.3 s → 0.4 s**. (It also corrects ADR 0003, which claimed "under 1 s" from
  a broken timer.)
- Subscription names now include the port. Locally every service is on
  127.0.0.1, so the old names collided.

## Consequences
- The order's final status comes quickly, but compensation continues after it
  (stock comes back a moment after CANCELLED). Consistency is eventual, not immediate.
- Refunds mean a customer can briefly be charged for a cancelled order. That's
  normal for real shops, but worth saying out loud.
- `ORDER_TIMEOUT_S` must be longer than the normal worst case, or healthy but
  slow orders get cancelled (and refunded).
