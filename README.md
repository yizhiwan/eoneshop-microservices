# EoneShop — learning microservices in public

A mini e-commerce shop built step by step, from a monolith to event-driven
microservices on Cloud Run. Live demo (coming): https://micro.eonelabs.my

| Phase | Topic | Status |
|---|---|---|
| 1 | Monolith baseline | ✅ |
| 2 | Split into services + API gateway | ✅ |
| 3 | Async events (Pub/Sub), idempotency, outbox | ✅ |
| 4 | Saga + compensation, chaos toggle | ✅ |
| 5 | Observability (OpenTelemetry, Cloud Trace) | |
| 6 | Deploy to Cloud Run | |
| 7 | Live event-flow visualizer | |

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
python scripts/dev.py            # all 6 services, no Docker; gateway on http://127.0.0.1:8080
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
