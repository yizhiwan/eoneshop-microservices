# EoneShop — learning microservices in public

A mini e-commerce shop built step by step, from a monolith to event-driven
microservices on Cloud Run, Google Pub/Sub and Neon Postgres.

**Live: https://micro.eonelabs.my**: place an order and watch it travel through
the services. Break it on purpose (declined card, slow payment, catalog down)
and watch the saga clean up. Failures only ever affect your own order.

| Phase | Topic | Status |
|---|---|---|
| 1 | Monolith baseline | ✅ |
| 2 | Split into services + API gateway | ✅ |
| 3 | Async events (Pub/Sub), idempotency, outbox | ✅ |
| 4 | Saga + compensation, chaos toggle | ✅ |
| 5 | Observability (OpenTelemetry, Cloud Trace) | ✅ |
| 6 | Deploy to Cloud Run (Pub/Sub, Neon, Cloud Trace) | ✅ [runbook](docs/deploy.md) |
| 7 | Live event-flow visualizer | ✅ |

## Run the monolith locally

```bash
cd monolith
python -m venv .venv && .venv/Scripts/python -m pip install -r requirements.txt
.venv/Scripts/python -m uvicorn app.main:app --reload
.venv/Scripts/python -m pytest -v
```

Place an order: `POST /orders {"product_id": 1, "qty": 2}`.
Orders totalling RM100+ fail payment on purpose and roll back stock.

## Run the services

```bash
python scripts/dev.py            # all 8 services, no Docker; gateway on http://127.0.0.1:8080
python scripts/dev.py --dupes    # broker delivers every message twice
docker compose up --build        # same thing in containers
```

```bash
curl -X POST 127.0.0.1:8080/api/orders -H 'content-type: application/json' \
     -H 'Idempotency-Key: my-first-order' -d '{"product_id":1,"qty":2}'   # 202 PENDING
curl 127.0.0.1:8080/api/orders/1          # COMPLETED a moment later
curl 127.0.0.1:8080/api/events            # every publish / delivery / retry
curl 127.0.0.1:8080/api/notifications     # the "emails"
```

```
order-svc --order.created--> catalog-svc --stock.reserved--> payment-svc
    ^                            |                               |
    +------ stock.rejected ------+       payment.succeeded/failed |
    +-------------------------------------------------------------+
order-svc --order.completed/cancelled--> notification-svc
            (all through pubsub-lite; every service has an outbox)
```

Each service owns its database and is tested alone.

## Break it on purpose (Phase 4)

Every way an order can fail ends in `order.cancelled`, and catalog and payment
undo their own step when they hear it (release stock, refund). Orders stuck in
PENDING are cancelled after `ORDER_TIMEOUT_S`.

```bash
curl -X PUT 127.0.0.1:8080/api/chaos/catalog -H 'content-type: application/json' -d '{"fail_rate": 1}'
curl -X PUT 127.0.0.1:8080/api/chaos/payment -H 'content-type: application/json' -d '{"latency_ms": 6000}'
curl -X PUT 127.0.0.1:8080/api/chaos/payment -H 'content-type: application/json' -d '{"decline_all": true}'
```

Or run all four failure scenarios and check stock always comes back:

```bash
ORDER_TIMEOUT_S=4 python scripts/dev.py --dupes
python scripts/saga_demo.py
```

Decisions are logged in [docs/adr](docs/adr).

## Follow one order (Phase 5)

Every order is one trace across all services, including through the outbox
and the broker. Retries show as errors and duplicates are tagged.

```bash
curl 127.0.0.1:8080/api/traces/by-order/my-first-order        # -> trace_id
curl 127.0.0.1:8080/api/traces/<trace_id>/waterfall
```

```
trace c967525d...  order trace-demo-flaky
    27.7ms     1.4ms █              order         publish order.created
    30.4ms     1.7ms █              catalog         process order.created  ✗
   548.5ms     1.3ms  █             catalog         process order.created  ✗
  1560.6ms    10.4ms    █           catalog         process order.created
  1654.8ms     1.8ms    █           catalog           publish stock.reserved
  1657.8ms     9.1ms    █           payment           process stock.reserved
  ...
```

Logs are JSON lines carrying `trace_id` (Cloud Logging format).
