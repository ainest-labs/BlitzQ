"""Workload definitions and the task bodies shared by both systems.

Every task receives ``(uid, t_enqueue, queue, *params)``. The body does the
workload's work and records the execution with :mod:`benchmarks.recorder`.
The *same* functions below are called by the BlitzQ and Celery task
wrappers, so the measured work is identical; only the queueing system differs.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any

from .recorder import record

KINDS = (
    "noop",
    "payload",
    "io",
    "cpu",
    "retry",
    "scheduled",
    "multiqueue",
    "crash",
    "interrupt",
    "results",
    "burst",
)


@dataclass
class Workload:
    name: str
    kind: str
    tasks: int = 10_000
    payload_bytes: int = 0
    io_ms: float = 10.0
    cpu_iters: int = 20_000
    #: Queue name -> share of tasks (multiqueue workload).
    queues: dict[str, float] = field(default_factory=lambda: {"default": 1.0})
    #: Retry workload: tasks with uid % fail_every == 0 fail their first attempt.
    fail_every: int = 2
    retry_delay: float = 0.1
    #: Scheduled workload: tasks are due this many seconds after enqueue.
    delay_s: float = 2.0
    #: Store task results (only the "results" workload).
    results: bool = False
    warmup: int = 500
    timeout: float = 300.0
    producers: int = 1
    #: Crash/interrupt workloads: act once this fraction of tasks completed.
    disrupt_at: float = 0.3
    #: Publish the whole workload before workers start consuming ("drain" test).
    #: Throughput is then measured from the first task start to the last task end,
    #: isolating worker capacity from producer speed.
    preload: bool = False

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Workload:
        return cls(**d)

    def payload(self) -> str:
        return "x" * self.payload_bytes if self.payload_bytes else ""

    def params(self) -> list[Any]:
        """Extra positional task parameters for this workload."""
        k = self.kind
        if k in ("payload", "burst", "results", "noop", "scheduled", "multiqueue", "crash",
                 "interrupt"):  # fmt: skip
            base: list[Any] = [self.payload()] if self.payload_bytes else []
            if k in ("crash", "interrupt"):
                return [*base, self.io_ms]
            return base
        if k == "io":
            return [self.io_ms]
        if k == "cpu":
            return [self.cpu_iters]
        if k == "retry":
            return [self.fail_every, self.retry_delay]
        raise ValueError(k)

    def task_name(self) -> str:
        return {
            "io": "bench.io",
            "cpu": "bench.cpu",
            "retry": "bench.retry",
            "crash": "bench.io",
            "interrupt": "bench.io",
        }.get(self.kind, "bench.noop")


# -- task bodies ---------------------------------------------------------------------
def noop_body(uid: int, t_enq: float, queue: str, *payload: Any) -> None:
    t0 = time.perf_counter()
    record(uid, "ok", 1, t_enq, t0, queue)


def io_body_sync(uid: int, t_enq: float, queue: str, *params: Any) -> None:
    t0 = time.perf_counter()
    time.sleep(params[-1] / 1000)
    record(uid, "ok", 1, t_enq, t0, queue)


async def io_body_async(uid: int, t_enq: float, queue: str, *params: Any) -> None:
    t0 = time.perf_counter()
    await asyncio.sleep(params[-1] / 1000)
    record(uid, "ok", 1, t_enq, t0, queue)


def cpu_body(uid: int, t_enq: float, queue: str, iters: int) -> int:
    t0 = time.perf_counter()
    acc = 0
    for i in range(iters):
        acc = (acc * 31 + i) % 1_000_003
    record(uid, "ok", 1, t_enq, t0, queue)
    return acc


class RetryNeeded(Exception):
    pass


def retry_body(uid: int, t_enq: float, queue: str, fail_every: int, attempt: int) -> None:
    """Fails the first attempt of every ``fail_every``-th task."""
    t0 = time.perf_counter()
    if attempt == 1 and uid % fail_every == 0:
        record(uid, "fail", attempt, t_enq, t0, queue)
        raise RetryNeeded(uid)
    record(uid, "ok", attempt, t_enq, t0, queue)
