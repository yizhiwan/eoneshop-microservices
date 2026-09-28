# ADR 0003 — Async events: broker, outbox, idempotent consumers

**Status:** accepted (2026-09-28)

## Context
Phase 2's synchronous chain made the client wait on every service, and a slow
dependency turned into a timeout or a wrong answer (ADR 0002). Phase 3 moves the
order flow onto events:

```
POST /orders -> order-svc: PENDING + order.created           (202, returns at once)
  order.created     -> catalog-svc: reserve -> stock.reserved | stock.rejected
  stock.reserved    -> payment-svc: charge  -> payment.succeeded | payment.failed
  stock.rejected / payment.*    -> order-svc: COMPLETED | CANCELLED -> order.completed | order.cancelled
  order.completed / .cancelled  -> notification-svc: "email"
```

## Decisions
1. **pubsub-lite broker** (`services/broker`) instead of the Pub/Sub emulator.
   It uses the real Pub/Sub publish REST shape and push envelope, so Phase 6 only
   changes a URL and adds auth. It copies Pub/Sub's semantics on purpose:
   at-least-once delivery, retry with backoff, dead-letters, no ordering.
   `DUPLICATE_RATE=1` delivers everything twice.
2. **Transactional outbox.** A service never publishes inside a request. It
   writes the event to an `outbox` table in the same DB transaction as the state
   change, and a relay thread publishes it. Without this, a crash between
   "commit" and "publish" loses the event, or a broker outage fails the request.
3. **Idempotent consumers.** Every handler records `event_id` in
   `processed_events` in the same transaction as its work. We use our own
   `event_id`, not Pub/Sub's `messageId`, because the outbox can re-publish the
   same event as a new message. The unique constraint, not the "seen it?"
   check, is the real guard: two concurrent copies both pass the check, and the
   loser's commit fails with IntegrityError, which we treat as a duplicate.
4. **Business-key idempotency where it matters:** payment keys charges by
   `order_ref`, so an order is never charged twice, even through two different events.
5. **Idempotency-Key header** on `POST /orders`: a retried request returns the
   same order (200) instead of creating a second one.
6. **Final states are final.** order-svc ignores events for orders that are no
   longer PENDING, because events can arrive late, twice, or out of order.
7. `shared/eventbus.py` holds **plumbing only** (outbox, envelope, dedupe).
   Event payloads stay owned by each service. A shared domain-model library
   would couple every deploy together again.

## Findings from the e2e run
- With `localhost` URLs an order took 14 s+ end to end. On Windows, `localhost`
  tries IPv6 `::1` first, and uvicorn only listens on IPv4, so each hop wasted
  about 2 s before falling back. 7 hops → 14 s. Local URLs now use `127.0.0.1`
  (all 3 orders final in under 1 s). This was also most of the 9 s in ADR 0002.
- Duplicate delivery on: 22 deliveries, 3 emails (not 6), 0 errors.

## Consequences / known gap
- The client gets `PENDING` and has to poll (or later, subscribe).
- **payment.failed no longer releases the reserved stock.** Phase 2 did that
  with a direct HTTP call. Phase 4 brings it back as a saga compensation step.
