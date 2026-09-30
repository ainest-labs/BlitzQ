"""In-process memory broker - **ephemeral**.

All state lives in the Python process. Everything is lost when the process
exits, and producers and workers must share the same process and event loop.
Intended for unit tests, notebooks and single-process scripts; never for
production work that must survive a restart.
"""

from __future__ import annotations

import asyncio
import heapq
import itertools
import time
from collections import deque
from collections.abc import Sequence
from typing import ClassVar

from .base import (
    Broker,
    Completion,
    Delivery,
    DeliveryGuarantee,
    PublishRequest,
    QueueStats,
    ScheduledEntry,
)


class MemoryBroker(Broker):
    guarantee: ClassVar[DeliveryGuarantee] = "ephemeral"
    supports_recovery: ClassVar[bool] = False

    def __init__(self) -> None:
        self._queues: dict[str, deque[bytes]] = {}
        self._cond: asyncio.Condition | None = None
        self._cond_loop: asyncio.AbstractEventLoop | None = None
        self._sched: dict[str, tuple[float, str, bytes]] = {}
        self._sched_heap: list[tuple[float, int, str]] = []
        self._seq = itertools.count()
        self._records: dict[str, tuple[bytes, float | None]] = {}
        self._dlq: dict[str, tuple[float, bytes]] = {}
        self._revoked: dict[str, float] = {}
        self._periodic: dict[str, float] = {}
        self._known: set[str] = set()
        self._workers: dict[str, tuple[bytes, float]] = {}

    def clone(self) -> MemoryBroker:
        # Sharing state is the only meaningful behaviour for an in-process broker.
        return self

    def _condition(self) -> asyncio.Condition:
        loop = asyncio.get_running_loop()
        if self._cond is None or self._cond_loop is not loop:
            self._cond = asyncio.Condition()
            self._cond_loop = loop
        return self._cond

    async def close(self) -> None:
        return None

    async def _notify(self) -> None:
        cond = self._condition()
        async with cond:
            cond.notify_all()

    def _q(self, name: str) -> deque[bytes]:
        q = self._queues.get(name)
        if q is None:
            q = self._queues[name] = deque()
        return q

    def _schedule(self, queue: str, task_id: str, data: bytes, eta: float) -> None:
        self._sched[task_id] = (eta, queue, data)
        heapq.heappush(self._sched_heap, (eta, next(self._seq), task_id))

    async def publish(self, requests: Sequence[PublishRequest]) -> None:
        now = time.time()
        for req in requests:
            if req.record is not None:
                self._set(req.task_id, req.record, req.record_ttl)
            if req.eta is not None and req.eta > now:
                self._schedule(req.queue, req.task_id, req.data, req.eta)
            else:
                self._q(req.queue).appendleft(req.data)
        await self._notify()

    async def prepare_consumer(self, queues: Sequence[str], consumer: str) -> None:
        self._known.update(queues)

    async def fetch(self, queue: str, count: int, timeout: float, consumer: str) -> list[Delivery]:
        q = self._q(queue)
        if not q and timeout > 0:
            cond = self._condition()
            async with cond:
                try:
                    await asyncio.wait_for(cond.wait_for(lambda: bool(q)), timeout)
                except TimeoutError:
                    return []
        out: list[Delivery] = []
        while q and len(out) < count:
            out.append(Delivery(queue, q.pop()))
        return out

    async def complete(self, completions: Sequence[Completion]) -> None:
        notify = False
        for c in completions:
            if c.record is not None and c.record_task_id is not None:
                self._set(c.record_task_id, c.record, c.record_ttl)
            if c.reschedule is not None:
                r = c.reschedule
                self._schedule(r.queue, r.task_id, r.data, r.eta)
            if c.dead_letter is not None:
                d = c.dead_letter
                self._dlq[d.task_id] = (time.time(), d.data)
            if c.requeue and c.delivery is not None:
                self._q(c.delivery.queue).append(c.delivery.data)
                notify = True
        if notify:
            await self._notify()

    async def promote_due(self, now: float, limit: int) -> tuple[int, float | None]:
        n = 0
        heap = self._sched_heap
        while heap and heap[0][0] <= now and n < limit:
            eta, _, tid = heapq.heappop(heap)
            entry = self._sched.get(tid)
            if entry is None or entry[0] != eta:
                continue  # cancelled or rescheduled
            del self._sched[tid]
            self._q(entry[1]).appendleft(entry[2])
            n += 1
        while heap and (heap[0][2] not in self._sched or self._sched[heap[0][2]][0] != heap[0][0]):
            heapq.heappop(heap)
        if n:
            await self._notify()
        return n, (heap[0][0] if heap else None)

    async def scheduled(self, limit: int = 100, offset: int = 0) -> list[ScheduledEntry]:
        items = sorted(self._sched.items(), key=lambda kv: kv[1][0])[offset : offset + limit]
        return [ScheduledEntry(tid, q, eta, data) for tid, (eta, q, data) in items]

    async def scheduled_count(self) -> int:
        return len(self._sched)

    async def is_scheduled(self, task_id: str) -> float | None:
        entry = self._sched.get(task_id)
        return entry[0] if entry else None

    async def cancel(self, task_id: str, revoke_ttl: int) -> bool:
        if self._sched.pop(task_id, None) is not None:
            return True
        self._revoked[task_id] = time.time() + revoke_ttl
        return False

    async def revoked(self) -> set[str]:
        now = time.time()
        for tid in [t for t, exp in self._revoked.items() if exp <= now]:
            del self._revoked[tid]
        return set(self._revoked)

    def _set(self, task_id: str, data: bytes, ttl: int | None) -> None:
        self._records[task_id] = (data, time.time() + ttl if ttl else None)

    async def get_record(self, task_id: str) -> bytes | None:
        entry = self._records.get(task_id)
        if entry is None:
            return None
        if entry[1] is not None and entry[1] <= time.time():
            del self._records[task_id]
            return None
        return entry[0]

    async def set_record(self, task_id: str, data: bytes, ttl: int | None) -> None:
        self._set(task_id, data, ttl)

    async def dead_letters(self, limit: int = 100, offset: int = 0) -> list[bytes]:
        items = sorted(self._dlq.values(), key=lambda v: v[0], reverse=True)
        return [d for _, d in items[offset : offset + limit]]

    async def dead_letter_count(self) -> int:
        return len(self._dlq)

    async def get_dead_letter(self, task_id: str) -> bytes | None:
        entry = self._dlq.get(task_id)
        return entry[1] if entry else None

    async def replay_dead_letter(self, task_id: str, queue: str, data: bytes) -> bool:
        if self._dlq.pop(task_id, None) is None:
            return False
        self._records.pop(task_id, None)
        self._q(queue).appendleft(data)
        await self._notify()
        return True

    async def purge_dead_letters(self) -> int:
        n = len(self._dlq)
        self._dlq.clear()
        return n

    async def claim_periodic(
        self, name: str, occurrence: float, queue: str, data: bytes | None
    ) -> bool:
        last = self._periodic.get(name)
        if last is not None and last >= occurrence:
            return False
        self._periodic[name] = occurrence
        if data is not None:
            self._q(queue).appendleft(data)
            await self._notify()
        return True

    async def periodic_last(self) -> dict[str, float]:
        return dict(self._periodic)

    async def queue_stats(self, queues: Sequence[str]) -> list[QueueStats]:
        return [QueueStats(q, waiting=len(self._queues.get(q, ()))) for q in queues]

    async def known_queues(self) -> set[str]:
        return set(self._known) | set(self._queues)

    async def register_worker(self, worker_id: str, info: bytes, ttl: int) -> None:
        self._workers[worker_id] = (info, time.time() + ttl)

    async def unregister_worker(self, worker_id: str) -> None:
        self._workers.pop(worker_id, None)

    async def workers(self) -> dict[str, bytes]:
        now = time.time()
        return {w: info for w, (info, exp) in self._workers.items() if exp > now}

    async def purge_queue(self, queue: str) -> int:
        q = self._queues.pop(queue, None)
        return len(q) if q else 0
