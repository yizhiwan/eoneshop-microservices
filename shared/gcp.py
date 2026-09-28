"""Tokens from the Cloud Run metadata server (ADR 0006).

On Cloud Run every service runs as its own service account, and the metadata
server hands out short-lived tokens for it: an access token to call Google APIs
(Pub/Sub), and an ID token to call another private Cloud Run service. No keys
are stored anywhere. Tokens are cached until shortly before they expire.
"""
import os
import threading
import time

import httpx

METADATA = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default"
_HEADERS = {"Metadata-Flavor": "Google"}
_cache: dict[str, tuple[str, float]] = {}
_lock = threading.Lock()


def on_cloud_run() -> bool:
    return "K_SERVICE" in os.environ


def _cached(key: str, fetch) -> str:
    with _lock:
        token, expires = _cache.get(key, ("", 0.0))
        if time.time() < expires - 60:
            return token
        token, lifetime = fetch()
        _cache[key] = (token, time.time() + lifetime)
        return token


def access_token() -> str:
    """OAuth access token for Google APIs, as this service's own account."""
    def fetch():
        r = httpx.get(f"{METADATA}/token", headers=_HEADERS, timeout=3.0)
        r.raise_for_status()
        body = r.json()
        return body["access_token"], float(body["expires_in"])
    return _cached("access", fetch)


def id_token(audience: str) -> str:
    """OIDC ID token for calling a private Cloud Run service at `audience`."""
    def fetch():
        r = httpx.get(f"{METADATA}/identity", params={"audience": audience},
                      headers=_HEADERS, timeout=3.0)
        r.raise_for_status()
        return r.text, 3600.0  # Google ID tokens live for one hour
    return _cached(f"id:{audience}", fetch)
