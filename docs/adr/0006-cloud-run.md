# ADR 0006 — Production on Cloud Run, Pub/Sub and Neon

**Status:** accepted (2026-09-28). Deployment pending owner-approved setup.

## Context
Everything so far ran on one laptop. Production needs to be public, cheap
(scale to zero), and keep every guarantee from Phases 3-5: outbox, idempotent
consumers, saga, traces.

## Decisions (owner-approved where marked)
1. **Google Pub/Sub** replaces pubsub-lite *(owner)*. The services already spoke
   its REST publish API and push envelope, so the publisher only gains a URL,
   a `TOPIC_PREFIX` (`eoneshop.`, because the project is shared) and a bearer token.
   Push subscriptions have 5 delivery attempts, backoff from 1 s to 60 s, then
   the `eoneshop.dead-letter` topic, matching what pubsub-lite simulated.
2. **Neon Postgres, one database per service** *(owner)*. SQLite in a container
   that Cloud Run wipes would silently break the outbox (unsent events lost),
   dedupe and tombstones (redelivered events reprocessed). Local dev stays on
   SQLite. **CI now runs the four database services against real Postgres 16**
   too: all green on the first run, including the 4-thread claim-first race test.
3. **Private services, identity-based calls.** Only the gateway is public. It
   calls the others with an ID token from the metadata server (audience = the
   target's URL), and Pub/Sub pushes with its own OIDC identity. Cloud Run
   IAM checks both, so there's no code to verify tokens and no shared secrets.
   Six service accounts, each least privilege: a service may publish only its
   own topics and read only its own database secret.
4. **Cloud Run throttles CPU between requests**, so a background thread
   can't be trusted to publish the outbox. `RELAY_INLINE=on` publishes right
   after each commit while the request still has CPU. The relay locks rows
   with `FOR UPDATE SKIP LOCKED`, so inline relays, the thread and several
   instances never take the same row.
5. **No scheduler: lazy timeouts.** A Cloud Scheduler tick every minute would
   keep the order database awake around the clock (Neon suspends after 5 idle
   minutes) and burn the free compute hours. Instead order-svc sweeps overdue
   orders whenever an order is placed or read (`LAZY_SWEEP=on`). A timeout
   only matters when someone is looking. `/internal/tick` remains for manual use.
6. **Cloud Trace + Cloud Logging** *(no collector to run)*: the OTel exporter
   switches with `OTEL_TRACES_EXPORTER=gcp`, and the JSON logs already carry the
   trace fields Cloud Logging links on.
7. **Cloud Build trigger** *(owner)*: `plan → build → deploy`, redeploying only
   services whose `services/<name>/` or `shared/` tree hash changed.
8. **Public demo guard rails:** per-IP write limit on the gateway (20/min per
   instance, a speed bump rather than security), `max-instances=2`, and chaos
   endpoints off (`CHAOS_ENABLED=off`) until Phase 7 designs a safe way to expose them.

## Consequences
- First request after idle pays both cold starts: Cloud Run (~1-2 s) and Neon (~0.5-1 s).
- Pub/Sub push has no delivery log like pubsub-lite's `/events`; the Phase 7
  visualizer will read Cloud Trace or a tap subscription instead.
- A stuck outbox row in catalog or payment is only retried on that service's
  next request. Pub/Sub's own retries make that rare, and it's a known gap.
- Per-service service accounts mean six identities to manage. That's worth it:
  a compromised catalog can't publish `payment.succeeded`.
