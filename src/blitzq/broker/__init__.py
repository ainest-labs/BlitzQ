"""Broker implementations.

``blitzq.broker.base.Broker`` is the contract; ``RedisFastBroker`` and
``RedisReliableBroker`` are the supported production backends and
``MemoryBroker`` is an ephemeral in-process backend for tests and scripts.
"""

from __future__ import annotations

from typing import Any, Literal

from .base import Broker, Delivery
from .memory import MemoryBroker
from .redis_fast import RedisFastBroker
from .redis_reliable import RedisReliableBroker

Mode = Literal["fast", "reliable"]

__all__ = [
    "Broker",
    "Delivery",
    "MemoryBroker",
    "RedisFastBroker",
    "RedisReliableBroker",
    "redis_broker",
]


def redis_broker(url: str, mode: Mode = "reliable", **options: Any) -> Broker:
    """Create a Redis broker for ``mode`` (``"fast"`` or ``"reliable"``)."""
    if mode == "reliable":
        return RedisReliableBroker(url, **options)
    if mode == "fast":
        return RedisFastBroker(url, **options)
    raise ValueError(f"unknown mode {mode!r}; expected 'fast' or 'reliable'")
