"""Runtime fault injection, so failures can be demonstrated on demand.

Every event-consuming service mounts `chaos.router` (GET/PUT /chaos) and calls
`chaos.disrupt()` before handling a push. A failing push is nacked, the broker
retries it, and after enough failures it is dead-lettered, which is exactly
what the saga's timeout has to cope with.
"""
import random
import time

from fastapi import APIRouter, HTTPException


class Chaos:
    def __init__(self, **extra):
        self.defaults = {"fail_rate": 0.0, "latency_ms": 0, **extra}
        self.settings = dict(self.defaults)
        self.router = APIRouter()
        self.router.add_api_route("/chaos", self.get, methods=["GET"])
        self.router.add_api_route("/chaos", self.put, methods=["PUT"])

    def get(self) -> dict:
        return self.settings

    def put(self, changes: dict) -> dict:
        unknown = set(changes) - set(self.defaults)
        if unknown:
            raise HTTPException(422, f"unknown chaos settings: {sorted(unknown)}")
        self.settings.update(changes)
        return self.settings

    def reset(self) -> None:
        self.settings = dict(self.defaults)

    def disrupt(self) -> None:
        if self.settings["latency_ms"]:
            time.sleep(self.settings["latency_ms"] / 1000)
        if random.random() < self.settings["fail_rate"]:
            raise HTTPException(503, "chaos: injected failure")
