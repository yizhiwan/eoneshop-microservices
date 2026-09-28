"""traces: a tiny OTLP/HTTP trace collector, for local dev and learning.

Services export spans here with the standard OTLP exporter
(OTEL_EXPORTER_OTLP_ENDPOINT). It keeps the last few hundred traces in memory
and can draw one as a text waterfall, which is enough to follow one order
across every service without running Jaeger. In Phase 6 the services export
to Cloud Trace instead; that is set up once in shared/telemetry.py.
"""
from collections import OrderedDict, defaultdict

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import PlainTextResponse
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
    ExportTraceServiceResponse,
)

MAX_TRACES = 300
STATUS_ERROR = 2

TRACES: OrderedDict[str, list[dict]] = OrderedDict()
BY_ORDER: dict[str, set[str]] = defaultdict(set)

app = FastAPI(title="traces")


def _value(v):
    kind = v.WhichOneof("value")
    return getattr(v, kind) if kind else None


def _attrs(kvs) -> dict:
    return {kv.key: _value(kv.value) for kv in kvs}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/v1/traces")
async def ingest(request: Request):
    req = ExportTraceServiceRequest()
    req.ParseFromString(await request.body())
    for rs in req.resource_spans:
        service = _attrs(rs.resource.attributes).get("service.name", "?")
        for ss in rs.scope_spans:
            for s in ss.spans:
                span = {
                    "trace_id": s.trace_id.hex(), "span_id": s.span_id.hex(),
                    "parent_id": s.parent_span_id.hex() or None, "name": s.name,
                    "service": service, "start_ns": s.start_time_unix_nano,
                    "end_ns": s.end_time_unix_nano,
                    "error": s.status.code == STATUS_ERROR or any(e.name == "exception" for e in s.events),
                    "attributes": _attrs(s.attributes),
                }
                TRACES.setdefault(span["trace_id"], []).append(span)
                TRACES.move_to_end(span["trace_id"])
                if ref := span["attributes"].get("order.ref"):
                    BY_ORDER[ref].add(span["trace_id"])
    while len(TRACES) > MAX_TRACES:
        TRACES.popitem(last=False)
    return Response(ExportTraceServiceResponse().SerializeToString(), media_type="application/x-protobuf")


def _summary(trace_id: str, spans: list[dict]) -> dict:
    start = min(s["start_ns"] for s in spans)
    end = max(s["end_ns"] for s in spans)
    root = min(spans, key=lambda s: (s["parent_id"] is not None, s["start_ns"]))
    refs = {s["attributes"].get("order.ref") for s in spans} - {None, ""}
    return {"trace_id": trace_id, "root": f"{root['service']}: {root['name']}",
            "order_ref": next(iter(refs), None), "spans": len(spans),
            "services": sorted({s["service"] for s in spans}),
            "errors": sum(s["error"] for s in spans),
            "duration_ms": round((end - start) / 1e6, 1)}


def _get(trace_id: str) -> list[dict]:
    if trace_id not in TRACES:
        raise HTTPException(404, "Trace not found (it may have been evicted)")
    return sorted(TRACES[trace_id], key=lambda s: s["start_ns"])


@app.get("/traces")
def list_traces(limit: int = 20):
    return [_summary(t, s) for t, s in reversed(list(TRACES.items())[-limit:])]


@app.get("/traces/by-order/{order_ref}")
def by_order(order_ref: str):
    return [_summary(t, TRACES[t]) for t in BY_ORDER.get(order_ref, ()) if t in TRACES]


@app.get("/traces/{trace_id}")
def get_trace(trace_id: str):
    return {**_summary(trace_id, _get(trace_id)), "span_list": _get(trace_id)}


@app.get("/traces/{trace_id}/waterfall", response_class=PlainTextResponse)
def waterfall(trace_id: str, width: int = 40):
    """The trace as text: one line per span, indented by parent, with a bar."""
    spans = _get(trace_id)
    by_id = {s["span_id"]: s for s in spans}
    children = defaultdict(list)
    for s in spans:
        parent = s["parent_id"] if s["parent_id"] in by_id else None
        children[parent].append(s)
    t0 = min(s["start_ns"] for s in spans)
    total = max(s["end_ns"] for s in spans) - t0 or 1

    lines = []

    def walk(span, depth):
        offset = (span["start_ns"] - t0) / total
        length = max((span["end_ns"] - span["start_ns"]) / total, 1 / width)
        bar = " " * int(offset * width) + "█" * max(1, round(length * width))
        flag = "  ✗" if span["error"] else ("  (dup)" if span["attributes"].get("event.duplicate") else "")
        lines.append(f"{(span['start_ns'] - t0) / 1e6:8.1f}ms {(span['end_ns'] - span['start_ns']) / 1e6:7.1f}ms "
                     f"{bar:<{width + 1}} {span['service']:<12} {'  ' * depth}{span['name']}{flag}")
        for child in sorted(children[span["span_id"]], key=lambda s: s["start_ns"]):
            walk(child, depth + 1)

    for root in sorted(children[None], key=lambda s: s["start_ns"]):
        walk(root, 0)
    s = _summary(trace_id, spans)
    header = (f"trace {trace_id}  order {s['order_ref']}\n{s['spans']} spans, {s['duration_ms']} ms, "
              f"{s['errors']} errors, services: {', '.join(s['services'])}\n")
    return header + "\n".join(lines) + "\n"
