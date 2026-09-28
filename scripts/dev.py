"""Run every service locally without Docker (one uvicorn process each).

    python scripts/dev.py              # gateway on http://127.0.0.1:8080
    python scripts/dev.py --dupes      # broker delivers every message twice

Ctrl+C stops everything. Needs the packages from any service's requirements.txt.
"""
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PORTS = {"gateway": 8080, "catalog": 8001, "order": 8002, "payment": 8003,
         "notification": 8004, "broker": 8085, "traces": 8086}
URL = {name: f"http://127.0.0.1:{port}" for name, port in PORTS.items()}
PUSH = "/pubsub/push"
SUBSCRIPTIONS = ",".join([
    f"order.created={URL['catalog']}{PUSH}",
    f"stock.reserved={URL['payment']}{PUSH}",
    f"stock.rejected={URL['order']}{PUSH}",
    f"payment.succeeded={URL['order']}{PUSH}",
    f"payment.failed={URL['order']}{PUSH}",
    f"order.completed={URL['notification']}{PUSH}",
    f"order.cancelled={URL['notification']}{PUSH}",
    # saga compensation (ADR 0004)
    f"order.cancelled={URL['catalog']}{PUSH}",
    f"order.cancelled={URL['payment']}{PUSH}",
])


def main() -> None:
    env = {**os.environ, "PYTHONPATH": str(ROOT), "PYTHONUNBUFFERED": "1", "BROKER_URL": URL["broker"],
           "CATALOG_URL": URL["catalog"], "ORDER_URL": URL["order"], "PAYMENT_URL": URL["payment"],
           "NOTIFICATION_URL": URL["notification"], "SUBSCRIPTIONS": SUBSCRIPTIONS,
           "DUPLICATE_RATE": "1" if "--dupes" in sys.argv else "0",
           # Tracing (ADR 0005): export to the local collector, flush quickly.
           "TRACES_URL": URL["traces"], "OTEL_EXPORTER_OTLP_ENDPOINT": URL["traces"],
           "OTEL_BSP_SCHEDULE_DELAY": "500"}
    procs = [
        subprocess.Popen([sys.executable, "-m", "uvicorn", "app.main:app", "--port", str(port),
                          "--log-level", "warning"], cwd=ROOT / "services" / name, env=env)
        for name, port in PORTS.items()
    ]
    print(f"EoneShop running: {URL['gateway']}  (events: /api/events, traces: /api/traces)")
    try:
        while all(p.poll() is None for p in procs):
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        for p in procs:
            p.terminate()


if __name__ == "__main__":
    main()
