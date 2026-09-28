"""Tracing and structured logs shared by every service (ADR 0005).

HTTP hops are traced automatically (FastAPI + httpx instrumentation, W3C
traceparent headers). Event hops are not HTTP-to-HTTP, so the trace context
travels inside the event instead: see EventBus.add / relay_once / handle_push.
"""
import json
import os
import sys

from opentelemetry import propagate, trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

SERVICE = "unknown"
_provider: TracerProvider | None = None

# Polling loops and fault-injection plumbing would drown the real traces.
EXCLUDED_URLS = "health,pubsub/push,chaos"


def setup(service: str, app=None) -> None:
    """Call once per process, before handling requests. Exports to Cloud Trace
    when OTEL_TRACES_EXPORTER=gcp, to an OTLP collector when
    OTEL_EXPORTER_OTLP_ENDPOINT is set, and nowhere otherwise (tests)."""
    global SERVICE, _provider
    SERVICE = service
    if _provider is None:
        _provider = TracerProvider(resource=Resource.create({"service.name": service}))
        if os.getenv("OTEL_TRACES_EXPORTER") == "gcp":
            # Production: Cloud Trace, authenticated as the service's own account.
            from opentelemetry.exporter.cloud_trace import CloudTraceSpanExporter
            _provider.add_span_processor(BatchSpanProcessor(CloudTraceSpanExporter()))
        elif os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"):
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
            _provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
        trace.set_tracer_provider(_provider)
        from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
        HTTPXClientInstrumentor().instrument()
    if app is not None:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        FastAPIInstrumentor.instrument_app(app, excluded_urls=EXCLUDED_URLS, exclude_spans=["receive", "send"])


def add_span_processor(processor: SpanProcessor) -> None:
    """For tests: capture spans in memory."""
    _provider.add_span_processor(processor)


def tracer() -> trace.Tracer:
    return trace.get_tracer(SERVICE)


def current_carrier() -> dict:
    """The active trace context as {'traceparent': ...}, to store or send along."""
    carrier: dict = {}
    propagate.inject(carrier)
    return carrier


def context_from(carrier: dict | None):
    return propagate.extract(carrier or {})


def log(message: str, severity: str = "INFO", **fields) -> None:
    """One JSON line per log. Cloud Logging reads `severity` and the trace
    fields, and links the log line to its trace in Cloud Trace."""
    entry = {"severity": severity, "service": SERVICE, "message": message, **fields}
    ctx = trace.get_current_span().get_span_context()
    if ctx.is_valid:
        trace_id, span_id = format(ctx.trace_id, "032x"), format(ctx.span_id, "016x")
        entry["trace_id"] = trace_id
        project = os.getenv("GOOGLE_CLOUD_PROJECT")
        if project:
            entry["logging.googleapis.com/trace"] = f"projects/{project}/traces/{trace_id}"
            entry["logging.googleapis.com/spanId"] = span_id
    # A single write per line: print() writes text and newline separately,
    # and lines from several processes sharing a pipe got spliced together.
    sys.stdout.write(json.dumps(entry) + "\n")
    sys.stdout.flush()
