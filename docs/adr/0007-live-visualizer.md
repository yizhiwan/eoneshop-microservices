# ADR 0007 — A live visualizer, with chaos scoped to one order

**Status:** accepted (2026-09-28)

## Context
The project should be showable: a visitor at micro.eonelabs.my places an
order and *sees* it travel through the services, including the failure cases
the saga handles. Two problems: in production there's no broker log to read
(pubsub-lite had `/events`, Google Pub/Sub doesn't), and the chaos switch from
ADR 0004 is global, so one visitor could break the shop for everyone.

## Decisions (owner-approved)
1. **A tap service, `feed`.** It subscribes to every topic and to the
   dead-letter topic, and keeps recent events in memory, per order. The page polls
   `GET /api/feed?order=<ref>&after=<seq>` about twice a second.
   - Nothing depends on it. If it's down or restarts, orders still work and only
     the animation is missing. So in-memory storage, one instance, and tap
     subscriptions with **no dead-lettering and 10-minute retention** (a sleeping
     feed never builds a backlog, and a tap failure is never an order failure).
   - Duplicates (at-least-once delivery) are shown once. Dead letters are shown
     with the attempt count and the failing service, read from the
     `CloudPubSubDeadLetterSource*` attributes Pub/Sub adds. pubsub-lite now
     forwards dead letters with the same attributes, so local and production match.
   - The page knows who receives each event from the subscription map, not from
     delivery logs. It shows publishes, not individual retries. Retries live in
     Cloud Trace (ADR 0005).
2. **Per-order scenarios instead of global chaos.** `POST /orders` accepts
   `scenario`: `normal`, `decline_payment`, `slow_payment` or `catalog_down`
   (validated). It travels inside that order's events, so it can only affect that order:
   - `decline_payment`: payment declines, the saga releases the stock.
   - `slow_payment`: payment waits `SLOW_PAYMENT_S` (28 s in production) before
     charging, longer than `ORDER_TIMEOUT_S` (20 s). It **commits its event claim
     first**, so it doesn't hold a transaction (or, on SQLite, the whole database)
     open while it waits. The cancel then leaves a tombstone and the late charge
     is blocked.
   - `catalog_down`: catalog fails every delivery of that order's
     `order.created`; Pub/Sub retries, then dead-letters it, and the order times out.
   The global chaos switch stays off in production.
3. **The page is served by the gateway** at `/`. It's a single self-contained HTML
   file with inline SVG and vanilla JS, no build step. It works in light and dark
   mode and at phone width, and respects `prefers-reduced-motion`.
4. **Timeline times come from the server clock**: each event's `occurred_at`
   minus the order's `created_at`. Otherwise every event in one poll batch showed
   the same time, and browser/server clock skew would have made gaps meaningless.
5. **The demo never sells out**: catalog tops a product back up to its seed stock
   when it drops below `RESTOCK_BELOW` (3 in production).

## Verified locally (every message delivered twice)
| Scenario | Result |
|---|---|
| normal | COMPLETED; hops at 0.0 / 0.1 / 0.2 / 0.4 s |
| decline_payment | CANCELLED (payment_declined), cancel fans out to 3 services, stock released |
| slow_payment | CANCELLED (timeout), stock released, the late charge never happened |
| catalog_down | dead-lettered after 5 deliveries (shown red on catalog), then CANCELLED (timeout) |

Also checked at 375 px wide in light mode: no horizontal scroll, and the page
scrolls to the diagram when an order is placed.

## Consequences
- One more private Cloud Run service (`shop-feed`, max 1 instance) and 10 more
  subscriptions. Both are free at demo traffic.
- A scenario is a request parameter, so anyone can trigger failures, but only on
  their own orders, and the gateway's per-IP write limit still applies.
