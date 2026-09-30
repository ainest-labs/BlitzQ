"""The scheduler process: periodic task dispatch and scheduled-task promotion.

Run with ``blitzq scheduler module:app``. Several scheduler instances may run
at the same time for availability: each periodic occurrence is claimed with an
atomic compare-and-set on the last dispatched occurrence (stored in the
broker), so it is dispatched at most once no matter how many schedulers race.

Periodic occurrence ids are deterministic (``periodic:<name>:<timestamp>``).

Restart recovery: on start (and on every tick) the scheduler reads the last
dispatched occurrence of each periodic task from the broker and applies the
task's missed-run policy to occurrences in between:

* ``run_once`` (default) - dispatch only the most recent missed occurrence.
* ``run_all`` - dispatch every missed occurrence (at most ``max_catchup``).
* ``skip`` - dispatch nothing that is more than ``grace`` seconds late.

Occurrences older than ``catchup_window`` are ignored. A periodic task that has
never been dispatched starts with the first occurrence after the scheduler
started (no backfill).

Wall-clock jumps: occurrences are computed from wall-clock time and the loop
re-evaluates at least every ``poll_interval`` seconds. A backwards jump never
re-dispatches an occurrence (the compare-and-set rejects it); a forward jump
is treated like downtime and handled by the missed-run policy.
"""

from __future__ import annotations

import asyncio
import signal
import threading
import time
from typing import TYPE_CHECKING

from .logs import logger
from .serialization import Envelope

if TYPE_CHECKING:
    from .client import PeriodicTask, Queue


class Scheduler:
    def __init__(
        self,
        app: Queue,
        *,
        promote: bool = True,
        poll_interval: float = 1.0,
        catchup_window: float = 7 * 86400,
        max_catchup: int = 100,
        grace: float | None = None,
    ) -> None:
        self.app = app
        self.broker = app.broker
        self.promote = promote
        self.poll_interval = poll_interval
        self.catchup_window = catchup_window
        self.max_catchup = max_catchup
        self.grace = grace if grace is not None else max(2.0, 2 * poll_interval)
        self._baseline: dict[str, float] = {}
        self._stop = asyncio.Event()
        self.dispatched = 0

    def stop(self) -> None:
        self._stop.set()

    def _occurrence_id(self, p: PeriodicTask, ts: float) -> str:
        return f"periodic:{p.name}:{ts:.3f}".rstrip("0").rstrip(".")

    def _message(self, p: PeriodicTask, ts: float) -> bytes:
        queue = p.queue or p.task.queue
        env = Envelope(
            id=self._occurrence_id(p, ts),
            task=p.task.name,
            queue=queue,
            args=list(p.args),
            kwargs=dict(p.kwargs),
            created_at=ts,
            enqueued_at=ts,
        )
        return self.app.serializer.encode_envelope(env)

    async def tick(self, now: float | None = None) -> int:
        """Dispatch due occurrences once. Returns the number dispatched by this instance."""
        now = time.time() if now is None else now
        periodic = self.app.periodic_tasks
        if not periodic:
            return 0
        last = await self.broker.periodic_last()
        sent = 0
        for p in periodic.values():
            prev = last.get(p.name)
            if prev is None:
                start = self._baseline.setdefault(p.name, now)
            else:
                start = max(prev, now - self.catchup_window)
            occs = p.schedule.occurrences(start, now, limit=self.max_catchup)
            if not occs:
                continue
            queue = p.queue or p.task.queue
            if p.missed == "run_all":
                todo = occs
            elif p.missed == "skip":
                latest = occs[-1]
                if now - latest > self.grace:
                    await self.broker.claim_periodic(p.name, latest, queue, None)
                    logger.info(
                        "skipped %d missed occurrence(s)", len(occs), extra={"periodic": p.name}
                    )
                    continue
                todo = [latest]
            else:
                todo = [occs[-1]]
                if len(occs) > 1:
                    logger.info(
                        "collapsing %d missed occurrences into one",
                        len(occs),
                        extra={"periodic": p.name},
                    )
            for ts in todo:
                if await self.broker.claim_periodic(p.name, ts, queue, self._message(p, ts)):
                    sent += 1
                    logger.info(
                        "dispatched periodic task",
                        extra={"periodic": p.name, "occurrence": ts, "lateness": now - ts},
                    )
        self.dispatched += sent
        return sent

    def _next_due(self, now: float) -> float:
        nxt = [p.schedule.next_after(now) for p in self.app.periodic_tasks.values()]
        return min(nxt) if nxt else now + self.poll_interval

    async def run(self) -> None:
        promoter = None
        if self.promote:
            promoter = asyncio.get_running_loop().create_task(self._promote_loop())
        logger.info(
            "scheduler started",
            extra={"periodic_tasks": len(self.app.periodic_tasks), "promote": self.promote},
        )
        try:
            while not self._stop.is_set():
                try:
                    await self.tick()
                except Exception:
                    logger.warning("scheduler tick failed", exc_info=True)
                now = time.time()
                wait = min(self.poll_interval, max(0.0, self._next_due(now) - now) + 0.001)
                try:
                    await asyncio.wait_for(self._stop.wait(), wait)
                except TimeoutError:
                    pass
        finally:
            if promoter is not None:
                promoter.cancel()
                await asyncio.gather(promoter, return_exceptions=True)
            logger.info("scheduler stopped", extra={"dispatched": self.dispatched})

    async def _promote_loop(self) -> None:
        backoff = 0.1
        while not self._stop.is_set():
            try:
                n, nxt = await self.broker.promote_due(time.time(), 1000)
            except Exception:
                logger.warning("promoting scheduled tasks failed", exc_info=True)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 5.0)
                continue
            backoff = 0.1
            if n >= 1000:
                continue
            wait = 0.5 if nxt is None else min(0.5, max(0.0, nxt - time.time()))
            await asyncio.sleep(wait)


def run_scheduler(scheduler: Scheduler) -> None:
    async def main() -> None:
        loop = asyncio.get_running_loop()
        sigs = [signal.SIGINT, signal.SIGTERM]
        if hasattr(signal, "SIGBREAK"):
            sigs.append(signal.SIGBREAK)
        for sig in sigs:
            try:
                loop.add_signal_handler(sig, scheduler.stop)
            except (NotImplementedError, RuntimeError):
                if threading.current_thread() is threading.main_thread():
                    signal.signal(sig, lambda *_: loop.call_soon_threadsafe(scheduler.stop))
        try:
            await scheduler.run()
        finally:
            await scheduler.app.close()

    asyncio.run(main())
