"""Functionality shared by the fast and reliable Redis brokers.

Key layout (``{ns}`` is the namespace, default ``blitzq``)::

    {ns}:l:{queue}      list    runnable messages (fast mode)
    {ns}:s:{queue}      stream  runnable messages (reliable mode, group "blitzq")
    {ns}:sched          zset    task id -> due time (delayed tasks and retries)
    {ns}:sched:d        hash    task id -> message
    {ns}:sched:q        hash    task id -> queue name
    {ns}:t:{id}         string  task record (state/result), with TTL
    {ns}:dlq            hash    task id -> dead-letter record
    {ns}:dlq:idx        zset    task id -> failure time
    {ns}:revoked        zset    task id -> revocation expiry
    {ns}:periodic       hash    periodic name -> last dispatched occurrence
    {ns}:queues         set     queue names seen by workers
    {ns}:workers        set     registered worker ids
    {ns}:w:{id}         string  worker info, with TTL (liveness)
"""

from __future__ import annotations

import asyncio
import time
import weakref
from collections.abc import Sequence
from typing import Any, ClassVar, Literal

from redis.asyncio import BlockingConnectionPool, Redis
from redis.asyncio.client import Pipeline
from redis.commands.core import AsyncScript

from . import _lua
from .base import Broker, Completion, PublishRequest, ScheduledEntry

Mode = Literal["l", "s"]
_SCRIPTS = ("PROMOTE", "CANCEL", "DLQ_ADD", "DLQ_REPLAY", "PERIODIC_CLAIM", "HEARTBEAT")


class RedisBrokerBase(Broker):
    """Common Redis plumbing. Use :class:`RedisFastBroker` or :class:`RedisReliableBroker`."""

    _mode: ClassVar[Mode]

    def __init__(
        self,
        url: str = "redis://localhost:6379/0",
        *,
        namespace: str = "blitzq",
        max_connections: int = 64,
        dead_letter_max: int = 100_000,
        **redis_options: Any,
    ) -> None:
        if not namespace or namespace.endswith(":"):
            raise ValueError("namespace must be a non-empty string without trailing ':'")
        self.url = url
        self.namespace = namespace
        self.max_connections = max_connections
        self.dead_letter_max = dead_letter_max
        self.redis_options = redis_options
        p = f"{namespace}:"
        self.prefix = p
        self.queue_prefix = f"{p}{self._mode}:"
        self.k_sched = f"{p}sched"
        self.k_sched_d = f"{p}sched:d"
        self.k_sched_q = f"{p}sched:q"
        self.k_dlq = f"{p}dlq"
        self.k_dlq_idx = f"{p}dlq:idx"
        self.k_revoked = f"{p}revoked"
        self.k_periodic = f"{p}periodic"
        self.k_queues = f"{p}queues"
        self.k_workers = f"{p}workers"
        # One client (connection pool) per event loop: redis-py async
        # connections belong to the loop that created them, and one Queue may
        # be used from several loops at once (a web loop, a worker thread, the
        # sync API's background loop). Entries vanish with their loop.
        self._conns: weakref.WeakKeyDictionary[
            asyncio.AbstractEventLoop, tuple[Redis, dict[str, AsyncScript]]
        ] = weakref.WeakKeyDictionary()

    # -- connection management -----------------------------------------------------
    def clone(self) -> RedisBrokerBase:
        return type(self)(
            self.url,
            namespace=self.namespace,
            max_connections=self.max_connections,
            dead_letter_max=self.dead_letter_max,
            **self.redis_options,
        )

    def _conn(self) -> tuple[Redis, dict[str, AsyncScript]]:
        loop = asyncio.get_running_loop()
        conn = self._conns.get(loop)
        if conn is None:
            opts: dict[str, Any] = {"socket_keepalive": True, "health_check_interval": 30}
            opts.update(self.redis_options)
            opts["decode_responses"] = False
            pool = BlockingConnectionPool.from_url(
                self.url, max_connections=self.max_connections, timeout=30, **opts
            )
            client = Redis(connection_pool=pool)
            scripts = {name: client.register_script(getattr(_lua, name)) for name in _SCRIPTS}
            conn = self._conns[loop] = (client, scripts)
        return conn

    def _r(self) -> Any:
        """Client for the running event loop.

        Typed as ``Any``: redis-py's stubs return ``bytes | str`` unions
        regardless of ``decode_responses``; this client always returns bytes.
        """
        return self._conn()[0]

    def _script(self, name: str) -> AsyncScript:
        return self._conn()[1][name]

    async def connect(self) -> None:
        await self._r().ping()

    async def ping(self) -> bool:
        return bool(await self._r().ping())

    async def close(self) -> None:
        """Close the connections owned by the running event loop."""
        conn = self._conns.pop(asyncio.get_running_loop(), None)
        if conn is not None:
            client = conn[0]
            await client.aclose()
            # The pool was passed in explicitly, so aclose() leaves it open.
            await client.connection_pool.disconnect()

    def qkey(self, queue: str) -> str:
        return self.queue_prefix + queue

    # -- helpers used by subclasses ------------------------------------------------
    def _push(self, pipe: Pipeline, key: str, data: bytes) -> None:
        raise NotImplementedError

    def _push_front(self, pipe: Pipeline, key: str, data: bytes) -> None:
        raise NotImplementedError

    def _add_publish(self, pipe: Pipeline, requests: Sequence[PublishRequest], now: float) -> None:
        # Records first, so a worker's "running" record is never overwritten by
        # a producer's "queued" record for the same message.
        for req in requests:
            if req.record is not None:
                pipe.set(f"{self.prefix}t:{req.task_id}", req.record, ex=req.record_ttl)
        for req in requests:
            if req.eta is not None and req.eta > now:
                self._add_schedule(pipe, req.queue, req.task_id, req.data, req.eta)
            else:
                self._push(pipe, self.qkey(req.queue), req.data)

    def _add_schedule(
        self, pipe: Pipeline, queue: str, task_id: str, data: bytes, eta: float
    ) -> None:
        pipe.hset(self.k_sched_d, task_id, data)
        pipe.hset(self.k_sched_q, task_id, queue)
        pipe.zadd(self.k_sched, {task_id: eta})

    async def _add_completion(self, pipe: Pipeline, c: Completion) -> None:
        if c.record is not None and c.record_task_id is not None:
            pipe.set(f"{self.prefix}t:{c.record_task_id}", c.record, ex=c.record_ttl)
        if c.reschedule is not None:
            r = c.reschedule
            self._add_schedule(pipe, r.queue, r.task_id, r.data, r.eta)
        if c.dead_letter is not None:
            d = c.dead_letter
            # Awaiting a script with client=pipe queues EVALSHA in the pipeline.
            await self._script("DLQ_ADD")(
                keys=[self.k_dlq, self.k_dlq_idx],
                args=[d.task_id, d.data, time.time(), d.max_entries or self.dead_letter_max],
                client=pipe,
            )
        if c.requeue and c.delivery is not None:
            self._push_front(pipe, self.qkey(c.delivery.queue), c.delivery.data)

    # -- queues --------------------------------------------------------------------
    async def publish(self, requests: Sequence[PublishRequest]) -> None:
        if not requests:
            return
        r = self._r()
        now = time.time()
        if len(requests) == 1:
            req = requests[0]
            if req.record is None and (req.eta is None or req.eta <= now):
                await self._publish_one(r, self.qkey(req.queue), req.data)
                return
        async with r.pipeline(transaction=True) as pipe:
            self._add_publish(pipe, requests, now)
            await pipe.execute()

    async def _publish_one(self, r: Any, key: str, data: bytes) -> None:
        raise NotImplementedError

    # -- schedule ------------------------------------------------------------------
    async def promote_due(self, now: float, limit: int) -> tuple[int, float | None]:
        self._r()
        res = await self._script("PROMOTE")(
            keys=[self.k_sched, self.k_sched_d, self.k_sched_q],
            args=[now, limit, self._mode, self.queue_prefix],
        )
        count = int(res[0])
        nxt = float(res[1]) if len(res) > 1 and res[1] else None
        return count, nxt

    async def scheduled(self, limit: int = 100, offset: int = 0) -> list[ScheduledEntry]:
        r = self._r()
        items = await r.zrange(self.k_sched, offset, offset + limit - 1, withscores=True)
        if not items:
            return []
        ids = [i for i, _ in items]
        async with r.pipeline(transaction=False) as pipe:
            pipe.hmget(self.k_sched_d, ids)
            pipe.hmget(self.k_sched_q, ids)
            datas, queues = await pipe.execute()
        out = []
        for (tid, eta), data, q in zip(items, datas, queues, strict=True):
            if data is None or q is None:
                continue  # promoted concurrently
            out.append(ScheduledEntry(tid.decode(), q.decode(), float(eta), data))
        return out

    async def scheduled_count(self) -> int:
        return int(await self._r().zcard(self.k_sched))

    async def is_scheduled(self, task_id: str) -> float | None:
        score = await self._r().zscore(self.k_sched, task_id)
        return None if score is None else float(score)

    # -- cancellation --------------------------------------------------------------
    async def cancel(self, task_id: str, revoke_ttl: int) -> bool:
        self._r()
        now = time.time()
        res = await self._script("CANCEL")(
            keys=[self.k_sched, self.k_sched_d, self.k_sched_q, self.k_revoked],
            args=[task_id, now + revoke_ttl, now],
        )
        return bool(res)

    async def revoked(self) -> set[str]:
        ids = await self._r().zrangebyscore(self.k_revoked, time.time(), "+inf")
        return {i.decode() for i in ids}

    # -- records -------------------------------------------------------------------
    async def get_record(self, task_id: str) -> bytes | None:
        res: bytes | None = await self._r().get(f"{self.prefix}t:{task_id}")
        return res

    async def set_record(self, task_id: str, data: bytes, ttl: int | None) -> None:
        await self._r().set(f"{self.prefix}t:{task_id}", data, ex=ttl)

    # -- dead letters --------------------------------------------------------------
    async def dead_letters(self, limit: int = 100, offset: int = 0) -> list[bytes]:
        r = self._r()
        ids = await r.zrevrange(self.k_dlq_idx, offset, offset + limit - 1)
        if not ids:
            return []
        datas = await r.hmget(self.k_dlq, ids)
        return [d for d in datas if d is not None]

    async def dead_letter_count(self) -> int:
        return int(await self._r().zcard(self.k_dlq_idx))

    async def get_dead_letter(self, task_id: str) -> bytes | None:
        res: bytes | None = await self._r().hget(self.k_dlq, task_id)
        return res

    async def replay_dead_letter(self, task_id: str, queue: str, data: bytes) -> bool:
        self._r()
        res = await self._script("DLQ_REPLAY")(
            keys=[self.k_dlq, self.k_dlq_idx, self.qkey(queue), f"{self.prefix}t:{task_id}"],
            args=[task_id, data, self._mode],
        )
        return bool(res)

    async def purge_dead_letters(self) -> int:
        r = self._r()
        async with r.pipeline(transaction=True) as pipe:
            pipe.zcard(self.k_dlq_idx)
            pipe.delete(self.k_dlq, self.k_dlq_idx)
            n, _ = await pipe.execute()
        return int(n)

    # -- periodic ------------------------------------------------------------------
    async def claim_periodic(
        self, name: str, occurrence: float, queue: str, data: bytes | None
    ) -> bool:
        self._r()
        res = await self._script("PERIODIC_CLAIM")(
            keys=[self.k_periodic, self.qkey(queue)],
            args=[
                name,
                repr(occurrence),
                self._mode,
                "1" if data is not None else "0",
                data or b"",
            ],
        )
        return bool(res)

    async def periodic_last(self) -> dict[str, float]:
        raw = await self._r().hgetall(self.k_periodic)
        return {k.decode(): float(v) for k, v in raw.items()}

    # -- workers / inspection ------------------------------------------------------
    async def register_worker(self, worker_id: str, info: bytes, ttl: int) -> None:
        async with self._r().pipeline(transaction=False) as pipe:
            pipe.sadd(self.k_workers, worker_id)
            pipe.set(f"{self.prefix}w:{worker_id}", info, ex=ttl)
            await pipe.execute()

    async def unregister_worker(self, worker_id: str) -> None:
        async with self._r().pipeline(transaction=False) as pipe:
            pipe.srem(self.k_workers, worker_id)
            pipe.delete(f"{self.prefix}w:{worker_id}")
            await pipe.execute()

    async def workers(self) -> dict[str, bytes]:
        r = self._r()
        ids = sorted(i.decode() for i in await r.smembers(self.k_workers))
        if not ids:
            return {}
        infos = await r.mget([f"{self.prefix}w:{i}" for i in ids])
        alive = {}
        dead = []
        for wid, info in zip(ids, infos, strict=True):
            if info is None:
                dead.append(wid)
            else:
                alive[wid] = info
        if dead:
            await r.srem(self.k_workers, *dead)
        return alive

    async def known_queues(self) -> set[str]:
        names = await self._r().smembers(self.k_queues)
        return {n.decode() for n in names}

    async def _remember_queues(self, queues: Sequence[str]) -> None:
        if queues:
            await self._r().sadd(self.k_queues, *queues)

    async def flush_namespace(self) -> int:
        """Delete every key in this namespace (testing and benchmarking helper)."""
        r = self._r()
        n = 0
        async for key in r.scan_iter(match=f"{self.prefix}*", count=1000):
            n += int(await r.delete(key))
        return n
