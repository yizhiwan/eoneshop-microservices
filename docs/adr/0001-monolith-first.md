# ADR 0001 — Start with a monolith

**Status:** accepted (2026-09-28)

## Context
The goal is to learn microservices. Splitting before the domain is understood
tends to produce the wrong service boundaries.

## Decision
Build one FastAPI app with three modules — `catalog`, `orders`, `payment` —
that already map to future services. Modules only talk through plain function
calls (`reserve_stock`, `release_stock`, `charge`), which become HTTP calls or
events later.

## Consequences
- Placing an order is one local DB transaction: stock, payment and order row
  succeed or roll back together. That guarantee is what we lose in Phase 3 and
  rebuild with a saga in Phase 4.
- Gives a baseline to compare latency and complexity against.
