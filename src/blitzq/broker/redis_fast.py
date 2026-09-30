"""Fast mode: Redis lists, at-most-once delivery.

Messages are removed from Redis when a worker pops them. This minimises
round-trips (one ``LPUSH`` per enqueue, one ``RPOP``/``BRPOP`` per batch of
dequeued messages, nothing on success unless results are stored), but a task
that is being executed - or is prefetched in memory - when its worker process
dies is **lost**. Graceful shutdown requeues unfinished work; crashes do not.

Use :class:`~blitzq.broker.redis_reliable.RedisReliableBroker` when tasks
must survive worker crashes.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, ClassVar

from redis.asyncio.client import Pipeline

from .base import Completion, Delivery, DeliveryGuarantee, QueueStats
from .redis_base import Mode, RedisBrokerBase


class RedisFastBroker(RedisBrokerBase):
    guarantee: ClassVar[DeliveryGuarantee] = "at-most-once"
    supports_recovery: ClassVar[bool] = False
    _mode: ClassVar[Mode] = "l"

    def _push(self, pipe: Pipeline, key: str, data: bytes) -> None:
        pipe.lpush(key, data)

    def _push_front(self, pipe: Pipeline, key: str, data: bytes) -> None:
        # Consumers pop from the right, so RPUSH puts the message next in line.
        pipe.rpush(key, data)

    async def _publish_one(self, r: Any, key: str, data: bytes) -> None:
        await r.lpush(key, data)

    async def prepare_consumer(self, queues: Sequence[str], consumer: str) -> None:
        await self._remember_queues(queues)

    async def fetch(self, queue: str, count: int, timeout: float, consumer: str) -> list[Delivery]:
        r = self._r()
        key = self.qkey(queue)
        if timeout <= 0:
            items = await r.rpop(key, count)
            if not items:
                return []
            return [Delivery(queue, data) for data in items]
        res = await r.brpop([key], timeout=timeout)
        if res is None:
            return []
        out = [Delivery(queue, res[1])]
        if count > 1:
            more = await r.rpop(key, count - 1)
            if more:
                out.extend(Delivery(queue, data) for data in more)
        return out

    async def complete(self, completions: Sequence[Completion]) -> None:
        todo = [
            c
            for c in completions
            if c.record is not None
            or c.reschedule is not None
            or c.dead_letter is not None
            or c.requeue
        ]
        if not todo:
            return
        async with self._r().pipeline(transaction=True) as pipe:
            for c in todo:
                await self._add_completion(pipe, c)
            await pipe.execute()

    async def queue_stats(self, queues: Sequence[str]) -> list[QueueStats]:
        if not queues:
            return []
        async with self._r().pipeline(transaction=False) as pipe:
            for q in queues:
                pipe.llen(self.qkey(q))
            lengths = await pipe.execute()
        return [QueueStats(q, waiting=int(n)) for q, n in zip(queues, lengths, strict=True)]

    async def purge_queue(self, queue: str) -> int:
        async with self._r().pipeline(transaction=True) as pipe:
            pipe.llen(self.qkey(queue))
            pipe.delete(self.qkey(queue))
            n, _ = await pipe.execute()
        return int(n)
