"""Feed the collector real OTLP payloads produced by the OpenTelemetry SDK."""
import pytest
from fastapi.testclient import TestClient
from opentelemetry.exporter.otlp.proto.common.trace_encoder import encode_spans
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import Status, StatusCode

from app import main


@pytest.fixture
def client():
    main.TRACES.clear()
    main.BY_ORDER.clear()
    return TestClient(main.app)


def make_spans(service: str, order_ref: str, fail: bool = False):
    exporter = InMemorySpanExporter()
    provider = TracerProvider(resource=Resource.create({"service.name": service}))
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = provider.get_tracer("test")
    with tracer.start_as_current_span("POST /orders", attributes={"order.ref": order_ref}):
        with tracer.start_as_current_span("publish order.created") as child:
            if fail:
                child.set_status(Status(StatusCode.ERROR))
    return exporter.get_finished_spans()


def post(client, spans):
    r = client.post("/v1/traces", content=encode_spans(spans).SerializeToString(),
                    headers={"content-type": "application/x-protobuf"})
    assert r.status_code == 200


def test_ingest_and_summarise(client):
    spans = make_spans("order", "r1")
    post(client, spans)
    [summary] = client.get("/traces").json()
    assert summary["order_ref"] == "r1" and summary["spans"] == 2
    assert summary["root"] == "order: POST /orders" and summary["services"] == ["order"]


def test_find_by_order_and_waterfall(client):
    post(client, make_spans("order", "r2", fail=True))
    [found] = client.get("/traces/by-order/r2").json()
    text = client.get(f"/traces/{found['trace_id']}/waterfall").text
    assert "POST /orders" in text and "  publish order.created  ✗" in text
    assert found["errors"] == 1


def test_unknown_trace_404(client):
    assert client.get("/traces/deadbeef/waterfall").status_code == 404
