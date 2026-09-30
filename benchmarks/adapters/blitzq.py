"""BlitzQ adapter.

Settings:
  processes     worker processes
  concurrency   concurrency per worker process
  mode          fast | reliable
  style         async | sync (task functions)
  threads       thread-pool size (default: concurrency)
  cpu_executor  thread | process
  producer      sync (one enqueue_sync call per task) | batch (async enqueue_many, 500/batch)
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Any

import redis

from .base import Adapter, Item, Producer


class BlitzQAdapter(Adapter):
    name = "blitzq"

    def env(self) -> dict[str, str]:
        return {
            **super().env(),
            "BENCH_BQ_MODE": self.s.get("mode", "reliable"),
            "BENCH_TASK_STYLE": self.s.get("style", "async"),
            "BENCH_CPU_EXECUTOR": self.s.get("cpu_executor", "thread"),
        }

    def worker_commands(self) -> list[list[str]]:
        c = int(self.s.get("concurrency", 100))
        cmd = [
            self.python(), "-m", "blitzq", "worker", "benchmarks.apps.blitzq_app:app",
            "-Q", ",".join(self.queues),
            "-c", str(c),
            "--threads", str(int(self.s.get("threads", c))),
            "--shutdown-timeout", "5",
            "--log-level", "WARNING",
        ]  # fmt: skip
        if self.s.get("process_pool"):
            cmd += ["--processes", str(self.s["process_pool"])]
        return [list(cmd) for _ in range(int(self.s.get("processes", 1)))]

    def backlog(self, r: redis.Redis) -> int:
        total = 0
        if self.s.get("mode", "reliable") == "fast":
            for q in self.queues:
                total += int(r.llen(f"blitzq:l:{q}"))  # type: ignore[arg-type]
        else:
            for q in self.queues:
                total += int(r.xlen(f"blitzq:s:{q}"))  # type: ignore[arg-type]
        total += int(r.zcard("blitzq:sched"))  # type: ignore[arg-type]
        return total

    def make_producer(self) -> Producer:
        from ..apps import blitzq_app

        return _BQProducer(blitzq_app.app, self.w.task_name(), self.s.get("producer", "sync"))


class _BQProducer(Producer):
    def __init__(self, app: Any, task_name: str, style: str) -> None:
        self.app = app
        self.task = app.tasks[task_name]
        self.style = style

    def produce(self, items: list[Item], t_enqueue_fn: Any, params: list[Any]) -> None:
        if self.style == "batch":
            asyncio.run(self._produce_batches(items, t_enqueue_fn, params))
            return
        task = self.task
        for uid, queue, delay in items:
            if queue == "default" and not delay:
                task.enqueue_sync(uid, t_enqueue_fn(), queue, *params)
            else:
                task.options(queue=queue, delay=delay or None).enqueue_sync(
                    uid, t_enqueue_fn(), queue, *params
                )

    async def _produce_batches(
        self, items: list[Item], t_enqueue_fn: Any, params: list[Any]
    ) -> None:
        batch_size = 500
        for i in range(0, len(items), batch_size):
            groups: dict[tuple[str, float], list[tuple[Any, ...]]] = defaultdict(list)
            for uid, queue, delay in items[i : i + batch_size]:
                groups[(queue, delay)].append((uid, t_enqueue_fn(), queue, *params))
            for (queue, delay), args in groups.items():
                await self.task.options(queue=queue, delay=delay or None).enqueue_many(args)
        await self.app.close()

    def close(self) -> None:
        self.app.close_sync()
