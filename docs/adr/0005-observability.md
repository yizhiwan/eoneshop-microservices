# ADR 0005 — Observability: one trace per order, across events

**Status:** accepted (2026-09-28)

## Context
Since Phase 3 one order touches five services through a broker, retries,
duplicates and compensations. Debugging so far meant grepping six logs and
matching `order_ref` by hand. We need to see one order as one picture.

## Decisions
1. **OpenTelemetry** in every service except the broker. FastAPI and httpx are
   auto-instrumented, so HTTP hops (client → gateway → order-svc) propagate the
   W3C `traceparent` header on their own.
2. **Trace context travels inside the event.** Events aren't HTTP-to-HTTP: they
   sit in an outbox, then go through a broker. `EventBus.add` stores the current
   `traceparent` in the event body, so it is committed atomically with the
   event itself. The relay opens a `publish <topic>` span under it (with
   `outbox.wait_ms`), and `handle_push` opens `process <topic>` under it on the
   consumer side. Result: **one order = one trace**, across all five services.
3. **Work without a request joins the order's trace.** order-svc stores the
   creating request's trace context on the order row, so the timeout sweeper's
   `saga timeout` span, and the whole compensation that follows, appear in the
   same trace instead of a new one.
4. **Failures and duplicates are visible:** chaos runs inside the `process`
   span, so every failed delivery is an error span and the broker's retries
   and backoff show on the timeline. Deduped deliveries are tagged
   `event.duplicate`.
5. **A 128-line local collector** (`services/traces`) instead of Jaeger (no
   Docker here). It takes standard OTLP/HTTP, keeps recent traces in memory, and
   draws a text waterfall: `GET /api/traces/by-order/<ref>`, then
   `GET /api/traces/<id>/waterfall`. In Phase 6 the exporter points at Cloud
   Trace, with no code change in the services.
6. **JSON logs with `trace_id`** (`telemetry.log`), in the shape Cloud Logging
   reads (`severity`, `logging.googleapis.com/trace`), so a log line links to
   its trace.
7. Health checks, `/chaos` and the relay's idle polling are excluded, and so
   are ASGI receive/send sub-spans. Otherwise noise drowns the useful spans.

## What the traces found
- **Duplicate side effects (a real bug since Phase 3).** A normal order's log
  showed "order completed" twice. Two copies of one event raced: both passed the
  "seen it?" check and both ran the handler, and the database rejected the
  second commit and rolled it back. The *data* was right (ADR 0003 counted email
  rows: 3, not 6), but **anything outside the database, like a real email or a
  log line, happened twice.**
  Fix, **claim first**: `first_time` now inserts *and flushes* the
  `processed_events` row before the handler runs. A concurrent copy blocks on
  that row's lock and then fails the unique constraint, *before* touching
  anything. New test: 4 threads deliver the same event at once. The old code
  sent more than one email in 5/5 runs; the new code sent one in 5/5.
- **Cold start on the first publish**: ~150 ms, the lazily created httpx client
  (ADR 0004 again). The relay now creates it at startup; first publish ~8 ms.
- **Log lines spliced together**: `print()` writes the text and the newline
  separately, and lines from seven processes sharing one pipe merged into
  invalid JSON. One `write()` per line fixed it.

## Consequences
- Every service carries the OTel SDK (~a few MB, and some startup time).
- A retried delivery creates a new `process` span each time, so busy traces
  get long. That's intended, because the retries are the story.
- Side effects can still repeat in one case: the handler succeeds, but its
  commit fails for another reason. Then the broker redelivers. At-least-once
  delivery means at-least-once side effects unless the side effect itself is
  idempotent (e.g. an email provider's idempotency key).
