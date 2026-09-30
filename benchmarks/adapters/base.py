"""Common adapter interface for benchmarked systems."""

from __future__ import annotations

import sys
from abc import ABC, abstractmethod
from typing import Any

import redis

from ..workloads import Workload

#: One enqueue: (uid, queue, delay_seconds)
Item = tuple[int, str, float]


class Adapter(ABC):
    name: str

    def __init__(self, settings: dict[str, Any], workload: Workload, broker_url: str) -> None:
        self.s = settings
        self.w = workload
        self.broker_url = broker_url

    @property
    def queues(self) -> list[str]:
        return list(self.w.queues)

    def env(self) -> dict[str, str]:
        """Environment for worker and producer processes."""
        return {
            "BLITZQ_BENCH_BROKER_URL": self.broker_url,
            "BENCH_RESULTS": "1" if self.w.results else "0",
            "BENCH_VIS": str(self.s.get("visibility_timeout", 10)),
            "BENCH_RECORD_SYNC": "1" if self.w.kind in ("crash", "interrupt") else "0",
        }

    @abstractmethod
    def worker_commands(self) -> list[list[str]]:
        """One command line per worker process."""

    @abstractmethod
    def backlog(self, r: redis.Redis) -> int:
        """Messages in the broker not yet completed (waiting + unacknowledged)."""

    @abstractmethod
    def make_producer(self) -> Producer:
        """Create the producer (called inside the producer subprocess)."""

    def describe(self) -> dict[str, Any]:
        return {"system": self.name, **self.s}

    @staticmethod
    def python() -> str:
        return sys.executable


class Producer(ABC):
    @abstractmethod
    def produce(self, items: list[Item], t_enqueue_fn: Any, params: list[Any]) -> None:
        """Enqueue ``items``. ``t_enqueue_fn()`` gives the enqueue timestamp to embed."""

    def close(self) -> None:  # noqa: B027
        pass
