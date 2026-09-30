"""Celery benchmark application, configured by environment variables.

BENCH_CELERY_ACKS_LATE  1 | 0   (1: ack after execution + reject_on_worker_lost,
                                 the at-least-once configuration)
BENCH_RESULTS           0 | 1   (Redis result backend)
BENCH_PREFETCH          worker_prefetch_multiplier
BENCH_VIS               broker visibility timeout (seconds)
"""

from __future__ import annotations

import os
from typing import Any

from celery import Celery

from .. import workloads as w

URL = os.environ.get("BLITZQ_BENCH_BROKER_URL", "redis://localhost:6379/0")
RESULTS = os.environ.get("BENCH_RESULTS", "0") == "1"
ACKS_LATE = os.environ.get("BENCH_CELERY_ACKS_LATE", "1") == "1"

app = Celery("bench", broker=URL, backend=URL if RESULTS else None)
app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    task_ignore_result=not RESULTS,
    result_expires=3600,
    task_acks_late=ACKS_LATE,
    task_reject_on_worker_lost=ACKS_LATE,
    worker_prefetch_multiplier=int(os.environ.get("BENCH_PREFETCH", "4")),
    broker_transport_options={"visibility_timeout": int(os.environ.get("BENCH_VIS", "10"))},
    broker_connection_retry_on_startup=True,
    worker_hijack_root_logger=False,
    task_default_queue="default",
    worker_send_task_events=False,
    task_send_sent_event=False,
)


@app.task(name="bench.noop")
def noop(uid: int, t_enq: float, queue: str, *payload: Any) -> None:
    w.noop_body(uid, t_enq, queue, *payload)


@app.task(name="bench.io")
def io(uid: int, t_enq: float, queue: str, *params: Any) -> None:
    w.io_body_sync(uid, t_enq, queue, *params)


@app.task(name="bench.cpu")
def cpu(uid: int, t_enq: float, queue: str, iters: int) -> int:
    return w.cpu_body(uid, t_enq, queue, iters)


@app.task(name="bench.retry", bind=True, max_retries=5)
def retry(self: Any, uid: int, t_enq: float, queue: str, fail_every: int, delay: float) -> None:
    try:
        w.retry_body(uid, t_enq, queue, fail_every, self.request.retries + 1)
    except w.RetryNeeded as exc:
        raise self.retry(exc=exc, countdown=delay) from None
