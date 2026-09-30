"""BlitzQ benchmark application, configured by environment variables.

BENCH_BQ_MODE       fast | reliable
BENCH_TASK_STYLE    async | sync   (sync = plain functions on the thread executor)
BENCH_CPU_EXECUTOR  thread | process
BENCH_RESULTS       0 | 1          (store task results)
BENCH_VIS           reliable-mode visibility timeout (seconds)
"""

from __future__ import annotations

import os
from typing import Any

from blitzq import Queue, Retry, current_task

from .. import workloads as w

MODE = os.environ.get("BENCH_BQ_MODE", "reliable")
STYLE = os.environ.get("BENCH_TASK_STYLE", "async")
RESULTS = os.environ.get("BENCH_RESULTS", "0") == "1"

app = Queue(
    "default",
    os.environ.get("BLITZQ_BENCH_BROKER_URL", "redis://localhost:6379/0"),
    mode=MODE,  # type: ignore[arg-type]
    store_results=RESULTS,
    visibility_timeout=float(os.environ.get("BENCH_VIS", "10")),
    max_deliveries=100,
)


def _attempt() -> int:
    ctx = current_task()
    return ctx.attempt if ctx else 1


if STYLE == "async":

    @app.task(name="bench.noop")
    async def noop(uid: int, t_enq: float, queue: str, *payload: Any) -> None:
        w.noop_body(uid, t_enq, queue, *payload)

    @app.task(name="bench.io")
    async def io(uid: int, t_enq: float, queue: str, *params: Any) -> None:
        await w.io_body_async(uid, t_enq, queue, *params)

    @app.task(name="bench.retry", retries=5)
    async def retry(uid: int, t_enq: float, queue: str, fail_every: int, delay: float) -> None:
        try:
            w.retry_body(uid, t_enq, queue, fail_every, _attempt())
        except w.RetryNeeded:
            raise Retry(delay=delay) from None

else:

    @app.task(name="bench.noop")
    def noop(uid: int, t_enq: float, queue: str, *payload: Any) -> None:  # type: ignore[misc]
        w.noop_body(uid, t_enq, queue, *payload)

    @app.task(name="bench.io")
    def io(uid: int, t_enq: float, queue: str, *params: Any) -> None:  # type: ignore[misc]
        w.io_body_sync(uid, t_enq, queue, *params)

    @app.task(name="bench.retry", retries=5)
    def retry(uid: int, t_enq: float, queue: str, fail_every: int, delay: float) -> None:  # type: ignore[misc]
        try:
            w.retry_body(uid, t_enq, queue, fail_every, _attempt())
        except w.RetryNeeded:
            raise Retry(delay=delay) from None


def cpu_task(uid: int, t_enq: float, queue: str, iters: int) -> int:
    return w.cpu_body(uid, t_enq, queue, iters)


cpu = app.task(name="bench.cpu", executor=os.environ.get("BENCH_CPU_EXECUTOR", "thread"))(  # type: ignore[arg-type]
    cpu_task
)
