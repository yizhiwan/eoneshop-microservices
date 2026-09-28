# EoneShop — learning microservices in public

A mini e-commerce shop built step by step, from a monolith to event-driven
microservices on Cloud Run. Live demo (coming): https://micro.eonelabs.my

| Phase | Topic | Status |
|---|---|---|
| 1 | Monolith baseline | ✅ |
| 2 | Split into services + API gateway | ⏳ |
| 3 | Async events (Pub/Sub), idempotency, outbox | |
| 4 | Saga + compensation, chaos toggle | |
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

Decisions are logged in [docs/adr](docs/adr).
