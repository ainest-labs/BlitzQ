"""Huey adapter.

Settings:
  processes    consumer processes
  concurrency  workers per consumer process (``-w``)
  worker_type  thread | greenlet | process (``-k``)
  producer     sync (one call per task; Huey has no batch publish API)

Huey has no reliable/at-least-once mode: it acknowledges (pops) a message
before executing it (``BLPOP``/``LPOP``), so it is always early-ack, comparable
only to BlitzQ fast mode / Celery's early-ack profile (``A-amo``/``B-amo``).
"""

from __future__ import annotations

from typing import Any

import redis

from .base import Adapter, Item, Producer


class HueyAdapter(Adapter):
    name = "huey"

    def env(self) -> dict[str, str]:
        return {**super().env(), "BENCH_HUEY_WORKER_TYPE": self.s.get("worker_type", "thread")}

    def worker_commands(self) -> list[list[str]]:
        # One Huey instance per queue (see benchmarks/apps/huey_app.py), so a
        # multi-queue workload needs one consumer per queue, not per process.
        cmds = []
        for queue in self.queues:
            attr = "app" if queue == "default" else f"huey_{queue}"
            cmd = [
                self.python(), "-m", "huey.bin.huey_consumer",
                f"benchmarks.apps.huey_app.{attr}",
                "-w", str(int(self.s.get("concurrency", 16))),
                "-k", self.s.get("worker_type", "thread"),
                "-q",
            ]  # fmt: skip
            cmds.extend(list(cmd) for _ in range(int(self.s.get("processes", 1))))
        return cmds

    def backlog(self, r: redis.Redis) -> int:
        total = sum(int(r.llen(f"huey.redis.{q}")) for q in self.queues)  # type: ignore[arg-type]
        total += sum(int(r.zcard(f"huey.schedule.{q}")) for q in self.queues)  # type: ignore[arg-type]
        return total

    def make_producer(self) -> Producer:
        from ..apps import huey_app

        return _HueyProducer(huey_app._instances, huey_app.TASKS, self.w.task_name())


class _HueyProducer(Producer):
    def __init__(
        self, instances: dict[str, Any], tasks: dict[str, dict[str, Any]], task_name: str
    ) -> None:
        self.wrappers = {q: tasks[q][task_name] for q in instances}
        self.instances = instances

    def produce(self, items: list[Item], t_enqueue_fn: Any, params: list[Any]) -> None:
        for uid, queue, delay in items:
            wrapper = self.wrappers[queue]
            args = (uid, t_enqueue_fn(), queue, *params)
            if delay:
                wrapper.schedule(args=args, delay=delay)
            else:
                wrapper(*args)

    def close(self) -> None:
        for h in self.instances.values():
            h.storage.close()
