"""Celery adapter.

Settings:
  processes     worker (celery) processes
  concurrency   pool size per worker process
  pool          prefork | threads | gevent | solo
  acks_late     true: task_acks_late + task_reject_on_worker_lost (at-least-once)
  prefetch      worker_prefetch_multiplier
  producer      sync (one apply_async call per task; Celery has no batch publish API)
"""

from __future__ import annotations

from typing import Any

import redis

from .base import Adapter, Item, Producer


class CeleryAdapter(Adapter):
    name = "celery"

    def env(self) -> dict[str, str]:
        return {
            **super().env(),
            "BENCH_CELERY_ACKS_LATE": "1" if self.s.get("acks_late", True) else "0",
            "BENCH_PREFETCH": str(self.s.get("prefetch", 4)),
        }

    def worker_commands(self) -> list[list[str]]:
        cmds = []
        for i in range(int(self.s.get("processes", 1))):
            cmds.append([
                self.python(), "-m", "celery", "-A", "benchmarks.apps.celery_app", "worker",
                "-P", self.s.get("pool", "prefork"),
                "-c", str(int(self.s.get("concurrency", 4))),
                "-Q", ",".join(self.queues),
                "-n", f"w{i}@%h",
                "--loglevel", "WARNING",
                "--without-gossip", "--without-mingle", "--without-heartbeat",
            ])  # fmt: skip
        return cmds

    def backlog(self, r: redis.Redis) -> int:
        total = sum(int(r.llen(q)) for q in self.queues)  # type: ignore[arg-type]
        total += int(r.hlen("unacked"))  # type: ignore[arg-type]
        return total

    def make_producer(self) -> Producer:
        from ..apps import celery_app

        return _CeleryProducer(celery_app.app, self.w.task_name())


class _CeleryProducer(Producer):
    def __init__(self, app: Any, task_name: str) -> None:
        self.app = app
        self.task = app.tasks[task_name]

    def produce(self, items: list[Item], t_enqueue_fn: Any, params: list[Any]) -> None:
        task = self.task
        for uid, queue, delay in items:
            task.apply_async(
                (uid, t_enqueue_fn(), queue, *params),
                queue=queue,
                countdown=delay or None,
            )

    def close(self) -> None:
        self.app.close()
