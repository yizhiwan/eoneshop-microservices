import json

import pytest
from fastapi.testclient import TestClient

from app.db import Base, engine
from app.main import app, bus
from shared.eventbus import make_push


@pytest.fixture
def client():
    Base.metadata.drop_all(engine)
    with TestClient(app) as c:
        yield c


@pytest.fixture
def published():
    sent = []
    bus.sender = lambda topic, body: sent.append((topic, json.loads(body)["data"]))

    def flush():
        bus.relay_once()
        return sent
    return flush


def place(client, **headers):
    return client.post("/orders", json={"product_id": 1, "qty": 2}, headers=headers)


def test_place_order_is_pending_and_emits_event(client, published):
    r = place(client)
    assert r.status_code == 202 and r.json()["status"] == "PENDING"
    ref = r.json()["ref"]
    assert published() == [("order.created", {"order_ref": ref, "product_id": 1, "qty": 2, "scenario": "normal"})]


def test_idempotency_key_returns_same_order(client, published):
    first = place(client, **{"Idempotency-Key": "k-123"})
    again = place(client, **{"Idempotency-Key": "k-123"})
    assert first.status_code == 202 and again.status_code == 200
    assert again.json()["id"] == first.json()["id"]
    assert len(published()) == 1


def test_payment_succeeded_completes(client, published):
    o = place(client).json()
    client.post("/pubsub/push", json=make_push("payment.succeeded", {"order_ref": o["ref"], "amount_cents": 5000}))
    got = client.get(f"/orders/{o['id']}").json()
    assert got["status"] == "COMPLETED" and got["total_cents"] == 5000
    assert published()[-1] == ("order.completed", {"order_ref": o["ref"], "status": "COMPLETED", "reason": None})


def test_stock_rejected_cancels(client):
    o = place(client).json()
    client.post("/pubsub/push", json=make_push("stock.rejected", {"order_ref": o["ref"], "reason": "out_of_stock"}))
    got = client.get(f"/orders/{o['id']}").json()
    assert got["status"] == "CANCELLED" and got["reason"] == "out_of_stock"


def test_late_event_does_not_reopen_final_order(client, published):
    o = place(client).json()
    client.post("/pubsub/push", json=make_push("payment.failed", {"order_ref": o["ref"], "amount_cents": 5000}))
    client.post("/pubsub/push", json=make_push("payment.succeeded", {"order_ref": o["ref"], "amount_cents": 5000}))
    assert client.get(f"/orders/{o['id']}").json()["status"] == "CANCELLED"
    assert [t for t, _ in published()] == ["order.created", "order.cancelled"]


def test_sweeper_cancels_stuck_orders(client, published):
    from datetime import datetime, timedelta, timezone

    from app.main import ORDER_TIMEOUT_S, sweep_once
    o = place(client).json()
    assert sweep_once() == 0
    later = datetime.now(timezone.utc) + timedelta(seconds=ORDER_TIMEOUT_S + 1)
    assert sweep_once(now=later) == 1
    got = client.get(f"/orders/{o['id']}").json()
    assert got["status"] == "CANCELLED" and got["reason"] == "timeout"
    assert published()[-1][0] == "order.cancelled"
    # the payment that finally arrives must not revive it (payment refunds instead)
    client.post("/pubsub/push", json=make_push("payment.succeeded", {"order_ref": o["ref"], "amount_cents": 5000}))
    assert client.get(f"/orders/{o['id']}").json()["status"] == "CANCELLED"


def test_trace_follows_the_order_through_outbox_and_events(client):
    """One trace id from the HTTP request, through the outbox, to the consumer."""
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    from shared import telemetry

    spans = InMemorySpanExporter()
    telemetry.add_span_processor(SimpleSpanProcessor(spans))
    trace_id = "ab" * 16
    traceparent = f"00-{trace_id}-{'cd' * 8}-01"

    o = place(client, traceparent=traceparent).json()
    bus.sender = lambda topic, body: None
    bus.relay_once()

    # payment answers in the same trace; order consumes it and emits order.completed
    client.post("/pubsub/push", json=make_push("payment.succeeded", {"order_ref": o["ref"], "amount_cents": 5000},
                                               trace={"traceparent": traceparent}))
    bus.relay_once()

    got = {s.name: format(s.context.trace_id, "032x") for s in spans.get_finished_spans()}
    assert got["POST /orders"] == trace_id
    assert got["publish order.created"] == trace_id
    assert got["process payment.succeeded"] == trace_id
    assert got["publish order.completed"] == trace_id
    process = next(s for s in spans.get_finished_spans() if s.name == "process payment.succeeded")
    assert process.attributes["order.ref"] == o["ref"]


def test_internal_tick_sweeps_and_relays(client, monkeypatch):
    from app import main as order_main
    sent = []
    bus.sender = lambda topic, body: sent.append(topic)
    place(client)
    monkeypatch.setattr(order_main, "ORDER_TIMEOUT_S", -1)  # everything is overdue
    assert client.post("/internal/tick").json() == {"swept": 1, "published": 2}
    assert sent == ["order.created", "order.cancelled"]


def test_lazy_sweep_cancels_when_someone_looks(client, monkeypatch):
    from app import main as order_main
    monkeypatch.setattr(order_main, "LAZY_SWEEP", True)
    o = place(client).json()
    assert client.get(f"/orders/{o['id']}").json()["status"] == "PENDING"
    monkeypatch.setattr(order_main, "ORDER_TIMEOUT_S", -1)
    got = client.get(f"/orders/{o['id']}").json()
    assert got["status"] == "CANCELLED" and got["reason"] == "timeout"


def test_flush_per_request_exports_the_server_span_before_responding():
    """Cloud Run pauses CPU after the response, so the request's own spans,
    including the outermost server span, must be exported before it ends."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    from shared import telemetry

    spans = InMemorySpanExporter()
    # A batch that would wait a minute: only an explicit flush exports it.
    telemetry.add_span_processor(BatchSpanProcessor(spans, schedule_delay_millis=60_000))
    mini = FastAPI()

    @mini.get("/ping")
    def ping():
        return {"ok": True}

    FastAPIInstrumentor.instrument_app(mini)
    telemetry.flush_per_request(mini)
    assert TestClient(mini).get("/ping").json() == {"ok": True}
    assert "GET /ping" in [s.name for s in spans.get_finished_spans()]


def test_scenario_travels_in_order_created(client, published):
    r = client.post("/orders", json={"product_id": 1, "qty": 1, "scenario": "slow_payment"})
    assert r.status_code == 202
    assert published()[0][1]["scenario"] == "slow_payment"
    assert client.post("/orders", json={"product_id": 1, "qty": 1, "scenario": "rm -rf"}).status_code == 422


def test_concurrent_sweeps_cancel_once(client, monkeypatch):
    """Found live in production: the page polls GET /orders every 600 ms and
    each GET runs the lazy sweep. Two overlapping sweeps both saw PENDING and
    both published order.cancelled (two event ids, so dedupe couldn't help)
    and the customer got two emails."""
    import threading

    from app import main as order_main

    sent = []
    bus.sender = lambda topic, body: sent.append(topic)
    place(client)
    bus.relay_once()
    monkeypatch.setattr(order_main, "ORDER_TIMEOUT_S", -1)
    start = threading.Barrier(4)

    def sweep():
        start.wait()
        order_main.sweep_once()

    threads = [threading.Thread(target=sweep) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    bus.relay_once()
    assert sent.count("order.cancelled") == 1
