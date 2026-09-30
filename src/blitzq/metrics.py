"""Low-overhead in-process metrics.

Counters and a fixed-bucket duration histogram are kept per queue (never per
task id, so label cardinality stays bounded by the number of queues). Updating
a metric is a dict lookup and an integer add. Workers publish periodic
snapshots to the broker for ``blitzq queue stats``; an optional Prometheus
exporter is available with ``pip install "blitzq[monitoring]"``.
"""

from __future__ import annotations

import bisect
from collections import defaultdict
from typing import Any

#: Execution-duration histogram bucket upper bounds, in seconds.
BUCKETS: tuple[float, ...] = (
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300,
)  # fmt: skip

EVENTS = (
    "received",
    "started",
    "succeeded",
    "failed",
    "retried",
    "dead_lettered",
    "timeouts",
    "cancelled",
    "recovered",
    "requeued",
    "malformed",
)


class _Histogram:
    __slots__ = ("count", "counts", "sum")

    def __init__(self) -> None:
        self.counts = [0] * (len(BUCKETS) + 1)
        self.count = 0
        self.sum = 0.0

    def observe(self, value: float) -> None:
        self.counts[bisect.bisect_left(BUCKETS, value)] += 1
        self.count += 1
        self.sum += value


class Metrics:
    def __init__(self) -> None:
        self.counters: defaultdict[tuple[str, str], int] = defaultdict(int)
        self.durations: defaultdict[str, _Histogram] = defaultdict(_Histogram)
        self.latency: defaultdict[str, _Histogram] = defaultdict(_Histogram)

    def inc(self, queue: str, event: str, n: int = 1) -> None:
        self.counters[(queue, event)] += n

    def observe_duration(self, queue: str, seconds: float) -> None:
        self.durations[queue].observe(seconds)

    def observe_latency(self, queue: str, seconds: float) -> None:
        """Enqueue-to-start latency (wall clock; subject to cross-host clock skew)."""
        self.latency[queue].observe(max(0.0, seconds))

    def snapshot(self) -> dict[str, Any]:
        queues: dict[str, dict[str, Any]] = {}
        for (queue, event), value in self.counters.items():
            queues.setdefault(queue, {})[event] = value
        for queue, h in self.durations.items():
            q = queues.setdefault(queue, {})
            q["duration_count"] = h.count
            q["duration_sum"] = h.sum
            q["duration_buckets"] = list(h.counts)
        for queue, h in self.latency.items():
            q = queues.setdefault(queue, {})
            q["latency_count"] = h.count
            q["latency_sum"] = h.sum
            q["latency_buckets"] = list(h.counts)
        return {"buckets": list(BUCKETS), "queues": queues}


def start_prometheus_exporter(metrics: Metrics, port: int, addr: str = "0.0.0.0") -> None:
    """Serve ``metrics`` on ``http://addr:port/metrics`` (requires ``prometheus-client``)."""
    try:
        from prometheus_client import start_http_server
        from prometheus_client.core import (
            REGISTRY,
            CounterMetricFamily,
            HistogramMetricFamily,
        )
    except ImportError as exc:  # pragma: no cover - depends on optional extra
        raise RuntimeError(
            'install "blitzq[monitoring]" to enable the Prometheus exporter'
        ) from exc

    class _Collector:
        def collect(self) -> Any:
            counter = CounterMetricFamily(
                "blitzq_tasks", "Task lifecycle events", labels=["queue", "event"]
            )
            for (queue, event), value in list(metrics.counters.items()):
                counter.add_metric([queue, event], value)
            yield counter
            for name, attr, help_ in (
                ("blitzq_task_duration_seconds", "durations", "Task execution time"),
                ("blitzq_queue_latency_seconds", "latency", "Enqueue-to-start latency"),
            ):
                hist = HistogramMetricFamily(name, help_, labels=["queue"])
                for queue, h in list(getattr(metrics, attr).items()):
                    cumulative, buckets = 0, []
                    for bound, c in zip([*BUCKETS, float("inf")], h.counts, strict=True):
                        cumulative += c
                        buckets.append(
                            (str(bound) if bound != float("inf") else "+Inf", cumulative)
                        )
                    hist.add_metric([queue], buckets, h.sum)
                yield hist

    REGISTRY.register(_Collector())
    start_http_server(port, addr=addr)
