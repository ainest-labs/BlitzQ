"""Huey benchmark application, configured by environment variables.

BENCH_RESULTS           0 | 1   (store task results)
BENCH_HUEY_WORKER_TYPE  thread | greenlet | process

Huey has no per-task queue concept like Celery/BlitzQ: one ``Huey`` instance is
one queue, each with its own Redis key namespace (``huey.redis.<name>``). The
"multi-queue" workload therefore uses one Huey instance per queue name.

The retry task uses ``context=True`` to receive its own ``Task`` object, whose
``retries`` counts down from the configured maximum; ``default_retries - task.retries
+ 1`` is this call's attempt number, matching the other systems' semantics.
``task.retry_delay`` is set dynamically so the delay comes from the workload
definition rather than being hard-coded per system.

With the ``greenlet`` worker type, ``huey_consumer.py`` does not monkey-patch
the standard library itself (unlike Celery's ``-P gevent``, which does), so
without patching ``time.sleep`` a synchronous task blocks every greenlet on
that worker rather than yielding. Patching must happen before ``socket``/``ssl``
are imported by anything else, so it runs at the top of this module, before
importing ``huey`` or ``redis``.
"""

from __future__ import annotations

import os

if os.environ.get("BENCH_HUEY_WORKER_TYPE") == "greenlet":
    from gevent import monkey

    monkey.patch_all()
from typing import Any

from huey import RedisHuey

from .. import workloads as w

URL = os.environ.get("BLITZQ_BENCH_BROKER_URL", "redis://localhost:6379/0")
RESULTS = os.environ.get("BENCH_RESULTS", "0") == "1"
QUEUES = ("default", "bulk", "normal", "urgent")

_instances = {name: RedisHuey(name, url=URL, results=RESULTS, utc=True) for name in QUEUES}
app = _instances["default"]  # imported by `huey_consumer.py benchmarks.apps.huey_app.app`

MAX_RETRIES = 5


def _register(h: RedisHuey) -> dict[str, Any]:
    @h.task(name="bench.noop")
    def noop(uid: int, t_enq: float, queue: str, *payload: Any) -> None:
        w.noop_body(uid, t_enq, queue, *payload)

    @h.task(name="bench.io")
    def io(uid: int, t_enq: float, queue: str, *params: Any) -> None:
        w.io_body_sync(uid, t_enq, queue, *params)

    @h.task(name="bench.cpu")
    def cpu(uid: int, t_enq: float, queue: str, iters: int) -> int:
        return w.cpu_body(uid, t_enq, queue, iters)

    @h.task(name="bench.retry", retries=MAX_RETRIES, context=True)
    def retry(uid: int, t_enq: float, queue: str, fail_every: int, delay: float, task: Any) -> None:
        attempt = MAX_RETRIES - task.retries + 1
        try:
            w.retry_body(uid, t_enq, queue, fail_every, attempt)
        except w.RetryNeeded:
            task.retry_delay = delay
            raise

    return {"bench.noop": noop, "bench.io": io, "bench.cpu": cpu, "bench.retry": retry}


#: {queue_name: {task_name: TaskWrapper}}, used by the adapter to publish.
TASKS = {name: _register(h) for name, h in _instances.items()}

# huey_consumer.py takes a dotted path to one Huey instance, and Huey has no
# concept of subscribing one consumer to several queues. Expose each queue's
# instance as a module attribute (huey_<name>) so the adapter can start one
# consumer process per queue for the multi-queue workload.
for _name, _inst in _instances.items():
    globals()[f"huey_{_name}"] = _inst
