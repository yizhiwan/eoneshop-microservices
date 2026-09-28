# ADR 0002 — Timeout budgets across service hops

**Status:** accepted (2026-09-28)

## Context
Phase 2 split the monolith into gateway → order → (catalog, payment) over HTTP.
End-to-end test with payment-svc stopped:

- order-svc alone took **~9 s** to answer: each refused connection costs ~2 s
  on Windows `localhost`, times 3 attempts (1 + 2 retries), plus the release call.
- the gateway had a **5 s** timeout, so it gave up first and returned **503**.
- but order-svc kept going and saved the order as **CANCELLED** and released the stock.

The client was told "failed, unknown", while the real outcome was a clean
cancellation. With a less friendly failure (e.g. payment slow but approving),
the client would be told 503 for an order that was actually charged.

## Decision
- The timeout of each hop must be **longer than the worst-case time of everything
  behind it**. Gateway: 15 s. order-svc to its dependencies: 3 s read / 1 s connect.
- Cut order-svc retries from 2 to 1: retries multiply latency at every hop.
- Retries only on connect errors (request never arrived), never on read timeouts,
  because a POST that timed out may already have happened.

## Consequences
- A synchronous chain is only as fast as its slowest link, and timeouts must be
  designed together, not per service.
- The real fix for "did it happen or not?" is idempotency (order `ref` as an
  idempotency key) and async events, which are Phase 3.
