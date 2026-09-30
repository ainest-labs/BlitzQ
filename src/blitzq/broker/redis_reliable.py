"""Reliable mode: Redis Streams consumer groups, at-least-once delivery.

* Each queue is a stream read through the consumer group ``blitzq``.
  ``XREADGROUP`` atomically assigns entries to a consumer and records them in
  the group's pending-entries list (PEL).
* A message is acknowledged (``XACK`` + ``XDEL``) only after the task
  finishes, in the same ``MULTI`` transaction as its result record, retry
  scheduling or dead-lettering.
* Workers renew the lease on in-flight entries with an ownership-checked
  ``XCLAIM ... JUSTID`` (see ``_lua.HEARTBEAT``). Entries idle for longer than
  the visibility timeout - because their worker crashed, hung or lost its
  connection - are reclaimed by live workers with ``XAUTOCLAIM``.
* A message whose delivery count exceeds ``max_deliveries`` is dead-lettered
  rather than redelivered forever (protection against poison messages that
  crash workers).

A task can therefore run more than once (for example if a worker dies after
executing a task but before its acknowledgement reached Redis). Tasks with
external side effects must be idempotent. See docs/delivery_guarantees.md.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, ClassVar

from redis.asyncio.client import Pipeline
from redis.exceptions import ResponseError

from .base import Completion, Delivery, DeliveryGuarantee, QueueStats
from .redis_base import Mode, RedisBrokerBase

GROUP = "blitzq"


class RedisReliableBroker(RedisBrokerBase):
    guarantee: ClassVar[DeliveryGuarantee] = "at-least-once"
    supports_recovery: ClassVar[bool] = True
    _mode: ClassVar[Mode] = "s"

    def _push(self, pipe: Pipeline, key: str, data: bytes) -> None:
        pipe.xadd(key, {"d": data})

    # Streams are append-only; a requeued message goes to the tail.
    _push_front = _push

    async def _publish_one(self, r: Any, key: str, data: bytes) -> None:
        await r.xadd(key, {"d": data})

    async def _ensure_group(self, queue: str) -> None:
        try:
            await self._r().xgroup_create(self.qkey(queue), GROUP, id="0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def prepare_consumer(self, queues: Sequence[str], consumer: str) -> None:
        for q in queues:
            await self._ensure_group(q)
        await self._remember_queues(queues)

    async def fetch(self, queue: str, count: int, timeout: float, consumer: str) -> list[Delivery]:
        r = self._r()
        key = self.qkey(queue)
        block = None if timeout <= 0 else max(1, int(timeout * 1000))
        try:
            res = await r.xreadgroup(GROUP, consumer, {key: ">"}, count=count, block=block)
        except ResponseError as exc:
            if "NOGROUP" not in str(exc):
                raise
            # Stream or group was deleted (e.g. queue purge); recreate and retry later.
            await self._ensure_group(queue)
            return []
        if not res:
            return []
        out: list[Delivery] = []
        for _key, entries in res:
            for entry_id, fields in entries:
                if fields:
                    out.append(Delivery(queue, fields[b"d"], entry_id, 1))
        return out

    def _ack(self, pipe: Pipeline, d: Delivery) -> None:
        key = self.qkey(d.queue)
        pipe.xack(key, GROUP, d.receipt)
        pipe.xdel(key, d.receipt)

    async def complete(self, completions: Sequence[Completion]) -> None:
        if not completions:
            return
        async with self._r().pipeline(transaction=True) as pipe:
            for c in completions:
                await self._add_completion(pipe, c)
                if (c.ack or c.requeue) and c.delivery is not None and c.delivery.receipt:
                    self._ack(pipe, c.delivery)
            await pipe.execute()

    async def heartbeat(
        self, queue: str, deliveries: Sequence[Delivery], consumer: str
    ) -> list[Delivery]:
        if not deliveries:
            return []
        self._r()
        by_id = {d.receipt: d for d in deliveries}
        lost = await self._script("HEARTBEAT")(
            keys=[self.qkey(queue)], args=[GROUP, consumer, *by_id.keys()]
        )
        return [by_id[i] for i in lost if i in by_id]

    async def recover(
        self, queue: str, consumer: str, idle_timeout: float, count: int
    ) -> list[Delivery]:
        if count <= 0:
            return []
        r = self._r()
        key = self.qkey(queue)
        min_idle = max(1, int(idle_timeout * 1000))
        claimed: list[tuple[Any, Any]] = []
        cursor: Any = "0-0"
        try:
            for _ in range(10):
                res = await r.xautoclaim(
                    key, GROUP, consumer, min_idle, start_id=cursor, count=count - len(claimed)
                )
                cursor, entries = res[0], res[1]
                claimed.extend(e for e in entries if e and e[1])
                if len(claimed) >= count or cursor in (b"0-0", "0-0"):
                    break
        except ResponseError as exc:
            if "NOGROUP" in str(exc):
                await self._ensure_group(queue)
                return []
            raise
        if not claimed:
            return []
        # Delivery counts drive poison-message detection. Recovery is a rare
        # path, so one pipelined XPENDING per claimed entry is acceptable.
        counts: dict[Any, int] = {}
        async with r.pipeline(transaction=False) as pipe:
            for eid, _ in claimed:
                pipe.xpending_range(key, GROUP, min=eid, max=eid, count=1)
            for rows in await pipe.execute():
                for p in rows:
                    counts[p["message_id"]] = int(p["times_delivered"])
        return [Delivery(queue, fields[b"d"], eid, counts.get(eid, 2)) for eid, fields in claimed]

    async def queue_stats(self, queues: Sequence[str]) -> list[QueueStats]:
        out: list[QueueStats] = []
        r = self._r()
        for q in queues:
            key = self.qkey(q)
            length = int(await r.xlen(key))
            waiting, pending = length, 0
            consumers = 0
            try:
                groups = await r.xinfo_groups(key)
            except ResponseError:
                groups = []
            for g in groups:
                name = g.get("name")
                if name in (GROUP, GROUP.encode()):
                    pending = int(g.get("pending") or 0)
                    consumers = int(g.get("consumers") or 0)
                    lag = g.get("lag")
                    waiting = int(lag) if lag is not None else max(0, length - pending)
            out.append(
                QueueStats(q, waiting=waiting, in_progress=pending, extra={"consumers": consumers})
            )
        return out

    async def purge_queue(self, queue: str) -> int:
        """Delete waiting messages. In-progress messages are also discarded."""
        key = self.qkey(queue)
        r = self._r()
        n = int(await r.xlen(key))
        await r.delete(key)
        await self._ensure_group(queue)
        return n
