"""Shared cache helpers with simple stampede protection."""

from __future__ import annotations

# Standard Library
import time
from collections.abc import Callable
from typing import TypeVar

# Django
from django.core.cache import cache

T = TypeVar("T")


def get_or_set_cache_with_lock(
    *,
    cache_key: str,
    ttl_seconds: int,
    loader: Callable[[], T],
    lock_ttl_seconds: int = 30,
    wait_timeout_seconds: float = 8.0,
    poll_interval_seconds: float = 0.2,
) -> T:
    """Get cache value or load once behind a short-lived distributed lock."""
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    lock_key = f"{cache_key}:lock"
    acquired = cache.add(lock_key, "1", lock_ttl_seconds)
    if acquired:
        try:
            value = loader()
            cache.set(cache_key, value, ttl_seconds)
            return value
        finally:
            cache.delete(lock_key)

    deadline = time.monotonic() + max(0.0, float(wait_timeout_seconds))
    interval = max(0.05, float(poll_interval_seconds))
    while time.monotonic() < deadline:
        time.sleep(interval)
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

    # Lock holder may have failed; attempt one more guarded load.
    acquired = cache.add(lock_key, "1", lock_ttl_seconds)
    if acquired:
        try:
            value = loader()
            cache.set(cache_key, value, ttl_seconds)
            return value
        finally:
            cache.delete(lock_key)

    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    return loader()
