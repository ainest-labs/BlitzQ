"""Functionality shared by the fast and reliable Redis brokers.

Key layout (``{ns}`` is the namespace, default ``blitzq``)::

    {ns}:l:{queue}      list    runnable messages (fast mode)
    {ns}:s:{queue}      stream  runnable messages (reliable mode, group "blitzq")
    {ns}:sched          zset    task id -> due time (delayed tasks and retries)
    {ns}:sched:d        hash    task id -> "queue\\0message" (packed together to
                                halve the commands spent scheduling/promoting)
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
import contextlib
import time
import weakref
from collections.abc import Callable, Sequence
from typing import Any, ClassVar, Literal

from redis.asyncio import BlockingConnectionPool, Redis
from redis.asyncio.client import Pipeline
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.commands.core import AsyncScript
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from . import _lua
from .base import Broker, Completion, PublishRequest, ScheduledEntry

Mode = Literal["l", "s"]
_SCRIPTS = (
    "PROMOTE",
    "CANCEL",
    "DLQ_ADD",
    "DLQ_REPLAY",
    "PERIODIC_CLAIM",
    "HEARTBEAT",
    "RATE_LIMIT",
    "IDEM_CLAIM",
    "IDEM_UNCLAIM",
    "IDEM_BEGIN",
    "IDEM_RENEW",
    "IDEM_FINISH",
)


def _pack_sched(queue: str, data: bytes) -> bytes:
    """Pack a scheduled entry's queue name and message into one hash field.

    Halves the Redis commands spent per delayed task, retry and periodic
    dispatch versus two separate hashes. Queue names cannot contain a NUL
    byte (BlitzQ never generates one; this isn't user-facing validation).
    """
    return queue.encode() + b"\x00" + data


def _unpack_sched(packed: bytes) -> tuple[str, bytes]:
    queue, _, data = packed.partition(b"\x00")
    return queue.decode(), data


class _NotifyHub:
    """One shared pub/sub connection per (broker, event loop), fanning out
    task-completion notifications to any number of local ``get_result()``
    waiters -- instead of each waiter opening its own dedicated Redis
    connection, which turns a burst of concurrent waiters into a burst of
    brand-new connections all arriving at once (thousands of tasks awaited
    together via ``asyncio.gather`` means thousands of connections, all
    fighting over Redis's single-threaded command loop to even get their
    first command serviced).

    A single ``PSUBSCRIBE {prefix}n:*`` catches every task's notification
    channel; incoming messages just set the ``asyncio.Event`` any local
    waiter registered for that task id. Same best-effort contract as before:
    a missed message just means the caller's own poll loop (in
    ``_get_result``) catches up on its next iteration.
    """

    def __init__(self, redis_factory: Callable[[], Any], pattern: str) -> None:
        self._redis_factory = redis_factory
        self._pattern = pattern
        self._waiters: dict[str, list[asyncio.Event]] = {}
        self._task: asyncio.Task[None] | None = None
        self._subscribed = asyncio.Event()

    def _ensure_started(self) -> None:
        if self._task is None or self._task.done():
            self._subscribed.clear()
            self._task = asyncio.ensure_future(self._run())

    async def _run(self) -> None:
        while True:
            pubsub = self._redis_factory().pubsub()
            try:
                await pubsub.psubscribe(self._pattern)
                self._subscribed.set()
                async for message in pubsub.listen():
                    if message.get("type") != "pmessage":
                        continue
                    channel = message["channel"]
                    if isinstance(channel, bytes):
                        channel = channel.decode()
                    task_id = channel.rsplit(":", 1)[-1]
                    for event in self._waiters.get(task_id, ()):
                        event.set()
            except asyncio.CancelledError:
                raise
            except Exception:
                # Connection hiccup: brief backoff, then resubscribe. Waiters
                # already registered keep waiting; they fall back to their
                # own poll loop via the timeout in `wait` below regardless.
                self._subscribed.clear()
                await asyncio.sleep(1.0)
            finally:
                try:
                    await pubsub.aclose()
                except Exception:
                    pass

    def register(self, task_id: str) -> asyncio.Event:
        """Register interest in ``task_id`` and return the Event that will
        be set on notification. Split from waiting so a caller can re-check
        the record in between: a task can finish and publish its
        notification in the gap between the caller's last check and this
        registration, in which case the hub discards it (no waiter existed
        yet) -- checking once more right after registering, instead of
        only after the full fallback timeout, is what keeps that race from
        costing seconds instead of microseconds.
        """
        self._ensure_started()
        event = asyncio.Event()
        self._waiters.setdefault(task_id, []).append(event)
        return event

    def unregister(self, task_id: str, event: asyncio.Event) -> None:
        events = self._waiters.get(task_id)
        if events is not None and event in events:
            events.remove(event)
            if not events:
                del self._waiters[task_id]

    async def ready(self, timeout: float) -> None:
        """Wait until the listener has actually subscribed, so a
        notification published right after ``register`` isn't missed
        simply because PSUBSCRIBE hadn't completed yet."""
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._subscribed.wait(), timeout=timeout)

    async def close(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None


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
        self.k_dlq = f"{p}dlq"
        self.k_dlq_idx = f"{p}dlq:idx"
        self.k_revoked = f"{p}revoked"
        self.k_periodic = f"{p}periodic"
        self.k_queues = f"{p}queues"
        self.k_workers = f"{p}workers"
        self.k_rate = f"{p}rl:"
        self.k_idem = f"{p}idem:"
        self.k_idem_lock = f"{p}idemlk:"
        # One client (connection pool) per event loop: redis-py async
        # connections belong to the loop that created them, and one Queue may
        # be used from several loops at once (a web loop, a worker thread, the
        # sync API's background loop). Entries vanish with their loop.
        self._conns: weakref.WeakKeyDictionary[
            asyncio.AbstractEventLoop, tuple[Redis, dict[str, AsyncScript]]
        ] = weakref.WeakKeyDictionary()
        # Same per-loop lifetime as _conns, for the same reason.
        self._hubs: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, _NotifyHub] = (
            weakref.WeakKeyDictionary()
        )

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
            opts: dict[str, Any] = {
                "socket_keepalive": True,
                "health_check_interval": 30,
                "retry": Retry(ExponentialBackoff(cap=1, base=0.05), retries=3),
                "retry_on_error": [RedisConnectionError, RedisTimeoutError],
            }
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

    def _hub(self) -> _NotifyHub:
        loop = asyncio.get_running_loop()
        hub = self._hubs.get(loop)
        if hub is None:
            hub = self._hubs[loop] = _NotifyHub(self._r, f"{self.prefix}n:*")
        return hub

    async def connect(self) -> None:
        await self._r().ping()

    async def ping(self) -> bool:
        return bool(await self._r().ping())

    async def close(self) -> None:
        """Close the connections owned by the running event loop."""
        loop = asyncio.get_running_loop()
        hub = self._hubs.pop(loop, None)
        if hub is not None:
            await hub.close()
        conn = self._conns.pop(loop, None)
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
        pipe.hset(self.k_sched_d, task_id, _pack_sched(queue, data))
        pipe.zadd(self.k_sched, {task_id: eta})

    def _rkey(self, task_id: str) -> str:
        """Pub/sub channel notified whenever task_id's record or dead-letter
        entry changes, so get_result() can wake up instead of polling."""
        return f"{self.prefix}n:{task_id}"

    async def _add_completion(self, pipe: Pipeline, c: Completion) -> None:
        if c.record is not None and c.record_task_id is not None:
            pipe.set(f"{self.prefix}t:{c.record_task_id}", c.record, ex=c.record_ttl)
            pipe.publish(self._rkey(c.record_task_id), b"1")
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
            pipe.publish(self._rkey(d.task_id), b"1")
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
            keys=[self.k_sched, self.k_sched_d],
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
        packed = await r.hmget(self.k_sched_d, ids)
        out = []
        for (tid, eta), p in zip(items, packed, strict=True):
            if p is None:
                continue  # promoted concurrently
            queue, data = _unpack_sched(p)
            out.append(ScheduledEntry(tid.decode(), queue, float(eta), data))
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
            keys=[self.k_sched, self.k_sched_d, self.k_revoked],
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

    async def wait_for_record(self, task_id: str, timeout: float) -> None:
        """Block up to ``timeout`` seconds for a notification on this task's
        channel, via the one shared pattern-subscribed connection for this
        (broker, event loop) -- see :class:`_NotifyHub`. However many tasks
        are being awaited at once in this process, this costs one Redis
        connection, not one per waiting call.
        """
        hub = self._hub()
        event = hub.register(task_id)
        try:
            await hub.ready(min(timeout, 5.0))
            # Close the register-vs-publish race: the notification could
            # have fired, and been discarded for lack of a waiter, in the
            # gap between the caller's last check and our registration
            # above. Recover in one extra round-trip instead of the full
            # fallback timeout.
            if await self.get_record(task_id) is not None:
                return
            await asyncio.wait_for(event.wait(), timeout=timeout)
        except (TimeoutError, asyncio.CancelledError):
            pass
        except Exception:
            # Connection hiccup: fall back to the caller's own poll loop
            # rather than raising out of what's meant to be best-effort.
            await asyncio.sleep(min(timeout, 1.0))
        finally:
            hub.unregister(task_id, event)

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

    # -- rate limiting ---------------------------------------------------------------
    async def check_rate_limit(self, key: str, rate: float, capacity: float, now: float) -> float:
        self._r()
        allowed, wait = await self._script("RATE_LIMIT")(
            keys=[f"{self.k_rate}{key}"], args=[now, rate, capacity]
        )
        return 0.0 if int(allowed) else float(wait)

    # -- idempotency ---------------------------------------------------------------
    async def idem_claim(self, key: str, task_id: str, ttl: int) -> str | None:
        self._r()
        prior = await self._script("IDEM_CLAIM")(keys=[f"{self.k_idem}{key}"], args=[task_id, ttl])
        return None if prior is None else bytes(prior).decode()

    async def idem_unclaim(self, key: str, task_id: str) -> None:
        self._r()
        await self._script("IDEM_UNCLAIM")(keys=[f"{self.k_idem}{key}"], args=[task_id])

    async def idem_begin(self, key: str, owner: str, lease: float) -> tuple[str, Any]:
        self._r()
        state, payload = await self._script("IDEM_BEGIN")(
            keys=[f"{self.k_idem}{key}", f"{self.k_idem_lock}{key}"],
            args=[owner, max(1, int(lease * 1000))],
        )
        name = bytes(state).decode()
        if name == "busy":
            return name, max(0.0, int(payload) / 1000)
        if name == "done":
            return name, bytes(payload)
        return name, None

    async def idem_renew(self, key: str, owner: str, lease: float) -> bool:
        self._r()
        ok = await self._script("IDEM_RENEW")(
            keys=[f"{self.k_idem_lock}{key}"], args=[owner, max(1, int(lease * 1000))]
        )
        return bool(int(ok))

    async def idem_finish(
        self, key: str, owner: str, task_id: str, success: bool, result: bytes, ttl: int
    ) -> None:
        self._r()
        await self._script("IDEM_FINISH")(
            keys=[f"{self.k_idem}{key}", f"{self.k_idem_lock}{key}"],
            args=[owner, 1 if success else 0, task_id, result, ttl],
        )

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
