"""The worker: fetches messages, executes tasks with bounded concurrency.

Execution model
---------------
* One asyncio event loop per worker process.
* ``async def`` tasks run as coroutines on that loop - thousands can be in
  flight without threads. They must not block (no synchronous I/O or long CPU
  work), or every task on the worker stalls.
* Regular functions run on a bounded thread pool (``threads``); use this for
  blocking I/O libraries.
* ``executor="process"`` tasks run on a process pool (``processes``); use this
  for CPU-bound work, which the GIL would otherwise serialise.

Concurrency and backpressure
----------------------------
``concurrency`` bounds the number of tasks executing at once in this worker;
``queue_concurrency`` additionally bounds individual queues. Each subscribed
queue has its own fetch loop, so a busy queue cannot starve a quiet one of
fetches. A fetch loop only takes messages from Redis when it has free slots
(it reserves them first), so excess work stays in Redis rather than piling up
in worker memory. When a queue is empty its loop blocks on Redis for at most
``block_timeout`` seconds holding one per-queue slot; a message received that
way waits in memory for a global slot (at most one per queue).

Completion writes (acks, results, retries, dead letters) are group-committed:
whatever accumulated while the previous batch was in flight is written in one
pipelined ``MULTI`` transaction.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import functools
import importlib
import logging
import os
import signal
import socket
import threading
import time
import traceback
import uuid
from collections import deque
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

import msgspec

from .broker.base import Completion, DeadLetterRequest, Delivery, Reschedule
from .context import TaskContext, _current
from .exceptions import Retry, SerializationError, TaskTimeout
from .logs import logger
from .metrics import Metrics
from .serialization import DeadLetter, Envelope
from .state import ErrorInfo, TaskInfo, TaskState

if TYPE_CHECKING:
    from .client import Queue
    from .task import Task

_TRACEBACK_LIMIT = 8000

# Heuristic for the "this looks CPU-bound" warning: a thread-executor task
# whose own thread spent at least this fraction of its wall-clock time on the
# CPU (rather than blocked in I/O), and took at least this long, is very
# likely competing for the GIL rather than waiting on the network or disk.
#
# The ratio threshold is deliberately low, not high: when several CPU-bound
# tasks run concurrently on the thread executor, each thread spends part of
# its own wall time waiting for the GIL rather than actually executing, which
# *lowers* its own cpu/wall ratio the more contended the worker is - measured
# as low as ~0.15 with 16 busy threads on a 12-core machine, and it falls
# further as thread:core ratio grows. A genuinely I/O-bound thread (blocked in
# a syscall, GIL released) measures ~0.0 regardless of concurrency, so 0.1
# keeps a wide margin from that while still catching contended CPU-bound
# tasks. See tests/unit/test_cpu_bound_warning.py.
# Priority levels within one queue. Every queue is checked at all three
# levels, in this order, on every fetch - "high" and "low" cost a couple of
# cheap empty non-blocking round-trips per batch when nobody uses them (see
# Worker._fetch_loop), never a whole extra fetch loop or a config flag to
# remember to flip on, so a task published with priority="high" is never
# silently stranded because the worker "wasn't listening" for it.
PRIORITY_LEVELS: tuple[str, ...] = ("high", "", "low")


def physical_queue(base: str, priority: str) -> str:
    """The physical broker queue name for ``priority`` ("high"/"normal"/"low") of ``base``."""
    level = "" if priority in ("", "normal") else priority
    return base if not level else f"{base}:{level}"


CPU_BOUND_MIN_SECONDS = 0.1
CPU_BOUND_CPU_RATIO = 0.1


class _CpuTimer:
    """Wall/CPU time of one thread-executor call, filled in after it returns."""

    __slots__ = ("cpu", "wall")

    def __init__(self) -> None:
        self.wall = 0.0
        self.cpu = 0.0


class _Limiter:
    """A counting limit whose capacity can be reserved in bulk."""

    __slots__ = ("_waiters", "limit", "used")

    def __init__(self, limit: int) -> None:
        if limit < 1:
            raise ValueError("concurrency limits must be >= 1")
        self.limit = limit
        self.used = 0
        self._waiters: deque[asyncio.Future[None]] = deque()

    @property
    def free(self) -> int:
        return self.limit - self.used

    async def wait(self) -> None:
        """Wait until at least one slot is free (does not take it)."""
        while self.used >= self.limit:
            fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
            self._waiters.append(fut)
            try:
                await fut
            finally:
                if not fut.done():
                    fut.cancel()

    async def acquire(self) -> None:
        await self.wait()
        self.used += 1

    def take(self, n: int) -> None:
        self.used += n

    def release(self, n: int = 1) -> None:
        self.used -= n
        while self._waiters:
            fut = self._waiters.popleft()
            if not fut.done():
                fut.set_result(None)


class _Flusher:
    """Group-commits completions to the broker from a single writer task."""

    def __init__(self, worker: Worker) -> None:
        self.worker = worker
        self.buf: list[Completion] = []
        self.event = asyncio.Event()
        self.idle = asyncio.Event()
        self.idle.set()
        self.task: asyncio.Task[None] | None = None
        self.closing = False

    def start(self) -> None:
        self.task = asyncio.get_running_loop().create_task(self._run(), name="blitzq-flusher")

    def submit(self, c: Completion) -> None:
        self.buf.append(c)
        self.idle.clear()
        self.event.set()

    async def _run(self) -> None:
        broker = self.worker.broker
        backoff = 0.05
        while True:
            await self.event.wait()
            self.event.clear()
            while self.buf:
                batch, self.buf = self.buf[:1000], self.buf[1000:]
                while True:
                    try:
                        await broker.complete(batch)
                        backoff = 0.05
                        break
                    except Exception:
                        if self.closing and self.worker._flush_deadline < time.monotonic():
                            logger.error(
                                "dropping %d unflushed completions at shutdown", len(batch)
                            )
                            break
                        logger.warning("completion write failed; retrying", exc_info=True)
                        await asyncio.sleep(backoff)
                        backoff = min(backoff * 2, 2.0)
            if not self.buf:
                self.idle.set()

    async def close(self, timeout: float) -> None:
        self.closing = True
        self.worker._flush_deadline = time.monotonic() + timeout
        try:
            await asyncio.wait_for(self.idle.wait(), timeout + 1)
        except TimeoutError:
            logger.error("timed out flushing %d completions", len(self.buf))
        if self.task is not None:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass


def _timed(fn: Callable[[], Any], timer: _CpuTimer) -> Any:
    """Run ``fn`` in the calling (executor) thread, recording its wall/CPU time."""
    t0 = time.monotonic()
    c0 = time.thread_time()
    try:
        return fn()
    finally:
        timer.wall = time.monotonic() - t0
        timer.cpu = time.thread_time() - c0


def _call_in_process(module: str, qualname: str, args: list[Any], kwargs: dict[str, Any]) -> Any:
    """Entry point for process-executor tasks: resolve the function by import path."""
    obj: Any = importlib.import_module(module)
    for part in qualname.split("."):
        obj = getattr(obj, part)
    fn = getattr(obj, "fn", obj)  # a Task object wraps the original function
    return fn(*args, **kwargs)


class Worker:
    """Consumes and executes tasks for one :class:`~blitzq.Queue` application.

    Parameters
    ----------
    queues:
        Queue names to consume; defaults to every queue used by registered
        tasks plus the default queue.
    concurrency:
        Maximum tasks executing at once in this process.
    queue_concurrency:
        Optional per-queue caps, e.g. ``{"images": 8}``.
    batch_size:
        Maximum messages fetched per round-trip (default: ``min(concurrency, 100)``).
    threads / processes:
        Sizes of the thread and process pools for sync and process tasks.
    shutdown_timeout:
        Seconds to wait for running tasks on graceful shutdown before cancelling
        them and returning their messages to the queue.
    schedule_poll_interval:
        Maximum delay between checks for due scheduled tasks (``promote=True``).
    warn_cpu_bound:
        Log a warning, once per task name, when a thread-executor task spends
        almost all of a non-trivial wall-clock duration on the CPU rather than
        blocked in I/O - a strong sign it is competing for the GIL and would
        run in true parallel on ``executor="process"`` instead. See
        docs/performance_tuning.md.
    """

    def __init__(
        self,
        app: Queue,
        *,
        queues: Sequence[str] | None = None,
        concurrency: int = 100,
        queue_concurrency: dict[str, int] | None = None,
        batch_size: int | None = None,
        threads: int | None = None,
        processes: int | None = None,
        block_timeout: float = 1.0,
        promote: bool = True,
        schedule_poll_interval: float = 0.5,
        shutdown_timeout: float = 30.0,
        heartbeat_interval: float | None = None,
        revocation_interval: float = 1.0,
        stats_interval: float = 2.0,
        warn_cpu_bound: bool = True,
        name: str | None = None,
        metrics: Metrics | None = None,
    ) -> None:
        self.app = app
        self.broker = app.broker
        if queues:
            self.queues = list(dict.fromkeys(queues))
        else:
            names = {app.name} | {t.queue for t in app.tasks.values()}
            self.queues = sorted(names)
        self.concurrency = concurrency
        self.global_limit = _Limiter(concurrency)
        qc = queue_concurrency or {}
        unknown = set(qc) - set(self.queues)
        if unknown:
            raise ValueError(f"queue_concurrency for unsubscribed queues: {sorted(unknown)}")
        self.queue_limits = {
            q: _Limiter(min(qc.get(q, concurrency), concurrency)) for q in self.queues
        }
        self.batch_size = batch_size or min(concurrency, 100)
        cpu = os.cpu_count() or 4
        self.threads = threads or min(concurrency, cpu * 4 + 4)
        self.processes = processes or cpu
        self.block_timeout = block_timeout
        self.promote = promote
        self.schedule_poll_interval = schedule_poll_interval
        self.shutdown_timeout = shutdown_timeout
        # Leases must be renewed well within the visibility timeout, or another
        # worker recovers (and re-executes) a task that is still running.
        self.heartbeat_interval = heartbeat_interval or app.visibility_timeout / 3
        if self.heartbeat_interval >= app.visibility_timeout / 2:
            raise ValueError(
                "heartbeat_interval must be less than half of the visibility timeout "
                f"({app.visibility_timeout}s)"
            )
        self.revocation_interval = revocation_interval
        self.stats_interval = stats_interval
        self.id = name or f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:6]}"
        self.metrics = metrics or Metrics()

        self._stopping = False
        self._stop_event: asyncio.Event | None = None
        self._fetchers: list[asyncio.Task[None]] = []
        self._background: list[asyncio.Task[None]] = []
        self._running: dict[str, asyncio.Task[None]] = {}
        # Strong references: asyncio keeps only weak references to tasks.
        self._inflight: set[asyncio.Task[None]] = set()
        self._cancel_reason: dict[str, str] = {}
        # One physical queue per priority level per subscribed queue (see
        # PRIORITY_LEVELS): leases/heartbeat/recovery are keyed by the physical
        # name (that's what the broker actually tracks), while execution
        # concurrency (queue_limits) stays keyed by the base name only, shared
        # across its priority levels.
        self._physical_queues: list[str] = [
            physical_queue(q, lvl) for q in self.queues for lvl in PRIORITY_LEVELS
        ]
        self._leases: dict[str, dict[Any, Delivery]] = {q: {} for q in self._physical_queues}
        self._revoked: set[str] = set()
        self._threads_pool: concurrent.futures.ThreadPoolExecutor | None = None
        self._process_pool: concurrent.futures.ProcessPoolExecutor | None = None
        self._flusher: _Flusher | None = None
        self._flush_deadline = 0.0
        self._promote_wake: asyncio.Event | None = None
        self._track = app.track_state
        self._ttl = app.result_ttl
        self._ser = app.serializer
        self._needs_ack = self.broker.supports_recovery
        self._started_at = 0.0
        self._sync_call: Callable[[Callable[[], Any]], Any] | None = None
        self.warn_cpu_bound = warn_cpu_bound
        self._cpu_warned: set[str] = set()
        self.processed = 0

    # -- lifecycle -----------------------------------------------------------------
    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        self._stop_event = asyncio.Event()
        self._promote_wake = asyncio.Event()
        self._started_at = time.time()
        await self.broker.prepare_consumer(self._physical_queues, self.id)
        # Load revocations before fetching so a new worker never runs a task
        # that was revoked while no worker was running.
        self._revoked = await self.broker.revoked()
        for hook in self.app.startup_hooks:
            res = hook()
            if asyncio.iscoroutine(res):
                await res
        self._build_sync_call()
        self._flusher = _Flusher(self)
        self._flusher.start()
        for q in self.queues:
            self._fetchers.append(loop.create_task(self._fetch_loop(q), name=f"blitzq-fetch-{q}"))
        if self.promote:
            self._background.append(loop.create_task(self._promote_loop(), name="blitzq-promote"))
        self._background.append(loop.create_task(self._revocation_loop(), name="blitzq-revoke"))
        self._background.append(loop.create_task(self._maintenance_loop(), name="blitzq-maint"))
        logger.info(
            "worker started",
            extra={
                "worker": self.id,
                "queues": ",".join(self.queues),
                "concurrency": self.concurrency,
                "guarantee": self.broker.guarantee,
            },
        )

    def stop(self) -> None:
        """Request graceful shutdown.

        Call from the worker's event loop; from another thread use
        ``loop.call_soon_threadsafe(worker.stop)``.
        """
        self._stopping = True
        if self._stop_event is not None:
            self._stop_event.set()

    async def run(self) -> None:
        """Start, run until :meth:`stop` is called, then shut down gracefully."""
        await self.start()
        assert self._stop_event is not None
        try:
            await self._stop_event.wait()
        finally:
            await self.shutdown()

    async def shutdown(self) -> None:
        self._stopping = True
        if self._promote_wake is not None:
            self._promote_wake.set()
        for t in self._background:
            t.cancel()
        await asyncio.gather(*self._background, return_exceptions=True)
        # Fetch loops are not cancelled mid-command (a popped message could be
        # lost); they exit after their current blocking fetch returns.
        if self._fetchers:
            _, pending = await asyncio.wait(self._fetchers, timeout=self.block_timeout + 5)
            for t in pending:
                t.cancel()
            await asyncio.gather(*self._fetchers, return_exceptions=True)
        inflight = list(self._inflight)
        if inflight:
            logger.info("waiting for %d running tasks", len(inflight))
            _, pending_tasks = await asyncio.wait(inflight, timeout=self.shutdown_timeout)
            if pending_tasks:
                logger.warning("cancelling %d tasks after shutdown timeout", len(pending_tasks))
                for tid, t in list(self._running.items()):
                    if t in pending_tasks:
                        self._cancel_reason[tid] = "shutdown"
                        t.cancel()
                await asyncio.wait(pending_tasks, timeout=5)
        if self._flusher is not None:
            await self._flusher.close(10.0)
        for hook in self.app.shutdown_hooks:
            try:
                res = hook()
                if asyncio.iscoroutine(res):
                    await res
            except Exception:
                logger.exception("shutdown hook failed")
        try:
            await self.broker.unregister_worker(self.id)
        except Exception:
            pass
        if self._threads_pool is not None:
            self._threads_pool.shutdown(wait=False, cancel_futures=True)
        if self._process_pool is not None:
            self._process_pool.shutdown(wait=False, cancel_futures=True)
        logger.info("worker stopped", extra={"worker": self.id, "processed": self.processed})

    # -- fetching ------------------------------------------------------------------
    async def _fetch_loop(self, base: str) -> None:
        qlim = self.queue_limits[base]
        glim = self.global_limit
        broker = self.broker
        consumer = self.id
        levels = [physical_queue(base, lvl) for lvl in PRIORITY_LEVELS]
        backoff = 0.1
        while not self._stopping:
            await qlim.wait()
            await glim.wait()
            if self._stopping:
                break
            n = min(qlim.free, glim.free, self.batch_size)
            qlim.take(n)
            glim.take(n)
            got: list[Delivery] = []
            remaining = n
            failed = False
            for physical in levels:
                if remaining <= 0:
                    break
                try:
                    batch = await broker.fetch(physical, remaining, 0, consumer)
                except Exception:
                    logger.warning("fetch failed on queue %s; backing off", physical, exc_info=True)
                    failed = True
                    break
                got.extend(batch)
                remaining -= len(batch)
            if failed:
                qlim.release(n)
                glim.release(n)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 5.0)
                continue
            if remaining > 0:
                qlim.release(remaining)
                glim.release(remaining)
            if not got:
                # Everything empty: block for one message holding only a queue
                # slot. Blocks on the base (normal) level only - a message
                # published only to :high/:low while the queue is otherwise
                # idle waits up to block_timeout longer, the same bounded
                # trade-off block_timeout already makes for a single queue.
                qlim.take(1)
                try:
                    batch = await broker.fetch(base, 1, self.block_timeout, consumer)
                except Exception:
                    qlim.release(1)
                    logger.warning("fetch failed on queue %s; backing off", base, exc_info=True)
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 5.0)
                    continue
                if not batch:
                    qlim.release(1)
                    continue
                try:
                    await glim.acquire()
                except asyncio.CancelledError:
                    # Cancelled at shutdown while holding a received message:
                    # hand it back to the broker instead of dropping it.
                    qlim.release(1)
                    for d in batch:
                        self._submit(Completion(delivery=d, ack=False, requeue=True))
                    raise
                got = batch
            backoff = 0.1
            for d in got:
                self._spawn(d, base)

    def _spawn(self, d: Delivery, base: str) -> None:
        if self._needs_ack:
            self._leases[d.queue][d.receipt] = d
        t = asyncio.get_running_loop().create_task(self._process(d, base))
        self._inflight.add(t)
        t.add_done_callback(self._inflight.discard)

    # -- execution -----------------------------------------------------------------
    async def _process(self, d: Delivery, base: str) -> None:
        physical = d.queue
        release_later: concurrent.futures.Future[Any] | asyncio.Future[Any] | None = None
        try:
            release_later = await self._execute(d)
        except BaseException:
            logger.exception("internal error while processing a message on %s", physical)
        finally:
            if self._needs_ack:
                self._leases[physical].pop(d.receipt, None)
            if release_later is not None and not release_later.done():
                # A timed-out thread/process call is still running: keep its
                # slot until it really finishes so concurrency stays bounded.
                release_later.add_done_callback(lambda _f: self._release(base))
            else:
                self._release(base)

    def _release(self, queue: str) -> None:
        self.queue_limits[queue].release()
        self.global_limit.release()

    def _submit(self, c: Completion) -> None:
        assert self._flusher is not None
        self._flusher.submit(c)

    async def _execute(self, d: Delivery) -> Any:
        queue = d.queue
        m = self.metrics
        m.inc(queue, "received")
        try:
            env = self._ser.decode_envelope(d.data)
        except SerializationError as exc:
            m.inc(queue, "malformed")
            logger.error("malformed message on %s dead-lettered: %s", queue, exc)
            self._dead_letter_raw(d, "malformed message", exc)
            return None
        task_id = env.id
        if d.delivery_count > self.app.max_deliveries:
            logger.error(
                "message delivered %d times; dead-lettering",
                d.delivery_count,
                extra={"task_id": task_id, "task": env.task},
            )
            self._terminal(d, env, None, "max deliveries exceeded", None, time.time())
            return None
        if task_id in self._revoked:
            self._cancelled(d, env)
            return None
        task = self.app.tasks.get(env.task)
        if task is None:
            logger.error("unknown task %r dead-lettered", env.task, extra={"task_id": task_id})
            self._terminal(d, env, None, "unknown task", None, time.time())
            return None

        opts = task.opts
        if opts.rate_limit is not None:
            wait = await self.broker.check_rate_limit(
                env.task, opts.rate_limit.rate, opts.rate_limit.count, time.time()
            )
            if wait > 0:
                self._defer_rate_limited(d, env, wait)
                return None
        started = time.time()
        m.inc(queue, "started")
        m.observe_latency(queue, started - env.enqueued_at)
        if self._track:
            self._submit(
                Completion(
                    ack=False,
                    record=self._ser.encode_info(
                        self._info(env, task, TaskState.RUNNING, started=started)
                    ),
                    record_task_id=task_id,
                    record_ttl=self._ttl,
                )
            )
        ctx = TaskContext(
            id=task_id,
            name=env.task,
            queue=queue,
            attempt=env.attempt,
            max_attempts=opts.max_attempts,
            correlation_id=env.correlation_id,
            headers=env.headers or {},
            enqueued_at=env.enqueued_at,
            worker=self.id,
        )
        timeout = env.timeout or opts.timeout
        t0 = time.perf_counter()
        token = _current.set(ctx)
        self._running[task_id] = asyncio.current_task()  # type: ignore[assignment]
        pending: Any = None
        cpu_timer: _CpuTimer | None = None
        try:
            if task.is_async:
                if timeout:
                    try:
                        async with asyncio.timeout(timeout) as cm:
                            result = await task.fn(*env.args, **env.kwargs)
                    except TimeoutError:
                        if cm.expired():
                            raise TaskTimeout(f"task exceeded {timeout}s") from None
                        raise
                else:
                    result = await task.fn(*env.args, **env.kwargs)
            else:
                fut, cpu_timer = self._run_blocking(task, env)
                pending = fut
                if timeout:
                    try:
                        result = await asyncio.wait_for(asyncio.shield(fut), timeout)
                    except TimeoutError:
                        if not fut.done():
                            raise TaskTimeout(f"task exceeded {timeout}s") from None
                        raise
                else:
                    result = await asyncio.shield(fut)
        except asyncio.CancelledError:
            reason = self._cancel_reason.pop(task_id, None)
            current = asyncio.current_task()
            if reason is None:
                raise
            if current is not None:
                current.uncancel()
            if reason == "revoked":
                self._cancelled(d, env)
            else:
                m.inc(queue, "requeued")
                self._submit(Completion(delivery=d, ack=False, requeue=True))
            return pending
        except BaseException as exc:
            finished = time.time()
            m.observe_duration(queue, time.perf_counter() - t0)
            if isinstance(exc, TaskTimeout):
                m.inc(queue, "timeouts")
            if not isinstance(exc, Exception):
                raise
            self._failure(d, env, task, exc, started, finished)
            return pending
        finally:
            _current.reset(token)
            if self._running.get(task_id) is asyncio.current_task():
                del self._running[task_id]
            self.processed += 1

        duration = time.perf_counter() - t0
        m.observe_duration(queue, duration)
        m.inc(queue, "succeeded")
        if cpu_timer is not None:
            self._maybe_warn_cpu_bound(env.task, cpu_timer)
        record = None
        if opts.store_result:
            finished = time.time()
            info = self._info(env, task, TaskState.SUCCEEDED, started, finished, result=result)
            try:
                record = self._ser.encode_info(info)
            except SerializationError as exc:
                info.result = None
                info.state = TaskState.FAILED
                info.error = _error_info(exc)
                record = self._ser.encode_info(info)
        if record is not None or self._needs_ack:
            self._submit(
                Completion(
                    delivery=d,
                    ack=True,
                    record=record,
                    record_task_id=task_id,
                    record_ttl=self._ttl,
                )
            )
        return None

    def _run_blocking(
        self, task: Task[Any, Any], env: Envelope
    ) -> tuple[asyncio.Future[Any], _CpuTimer | None]:
        loop = asyncio.get_running_loop()
        if task.opts.executor == "process":
            if self._process_pool is None:
                self._process_pool = concurrent.futures.ProcessPoolExecutor(self.processes)
            fn = task.fn
            fut = loop.run_in_executor(
                self._process_pool,
                _call_in_process,
                fn.__module__,
                fn.__qualname__,
                env.args,
                env.kwargs,
            )
            return fut, None
        if self._threads_pool is None:
            self._threads_pool = concurrent.futures.ThreadPoolExecutor(
                self.threads, thread_name_prefix="blitzq-task"
            )
        call = functools.partial(task.fn, *env.args, **env.kwargs)
        wrapped = self._sync_call
        target: Callable[[], Any] = call if wrapped is None else functools.partial(wrapped, call)
        ctx = contextvars.copy_context()
        timer = _CpuTimer() if self.warn_cpu_bound else None
        if timer is not None:
            target = functools.partial(_timed, target, timer)
        return loop.run_in_executor(self._threads_pool, ctx.run, target), timer

    def _maybe_warn_cpu_bound(self, task_name: str, timer: _CpuTimer) -> None:
        if task_name in self._cpu_warned or timer.wall < CPU_BOUND_MIN_SECONDS:
            return
        if timer.cpu < timer.wall * CPU_BOUND_CPU_RATIO:
            return  # spent meaningful time blocked (I/O), not just on the CPU
        self._cpu_warned.add(task_name)
        logger.warning(
            "task %r looks CPU-bound (%.0fms wall, %.0f%% on CPU) but is running on the "
            "thread executor, where Python's GIL serialises it against every other task "
            'on this worker; consider @queue.task(executor="process") instead',
            task_name,
            timer.wall * 1000,
            100 * timer.cpu / timer.wall,
            extra={"task": task_name},
        )

    def _build_sync_call(self) -> None:
        wrappers = list(self.app.sync_wrappers)
        if not wrappers:
            self._sync_call = None
            return

        def call(fn: Callable[[], Any]) -> Any:
            inner: Callable[[], Any] = fn
            for w in reversed(wrappers):
                inner = functools.partial(w, inner)
            return inner()

        self._sync_call = call

    # -- outcomes ------------------------------------------------------------------
    def _info(
        self,
        env: Envelope,
        task: Task[Any, Any] | None,
        state: TaskState,
        started: float | None = None,
        finished: float | None = None,
        *,
        result: Any = None,
        error: ErrorInfo | None = None,
        next_attempt_at: float | None = None,
    ) -> TaskInfo:
        return TaskInfo(
            id=env.id,
            state=state,
            task=env.task,
            queue=env.queue,
            attempt=env.attempt,
            max_attempts=task.opts.max_attempts if task else env.attempt,
            created_at=env.created_at,
            enqueued_at=env.enqueued_at,
            started_at=started,
            finished_at=finished,
            result=result,
            error=error,
            correlation_id=env.correlation_id,
            worker=self.id,
            next_attempt_at=next_attempt_at,
        )

    def _failure(
        self,
        d: Delivery,
        env: Envelope,
        task: Task[Any, Any],
        exc: Exception,
        started: float,
        finished: float,
    ) -> None:
        opts = task.opts
        policy = opts.retry_policy
        retryable = policy.is_retryable(exc)
        extra = {"task_id": env.id, "task": env.task, "queue": env.queue, "attempt": env.attempt}
        if retryable and env.attempt < opts.max_attempts:
            if isinstance(exc, Retry) and exc.delay is not None:
                delay = max(0.0, exc.delay)
            else:
                delay = policy.compute_delay(env.attempt)
            eta = time.time() + delay
            nxt = msgspec.structs.replace(env, attempt=env.attempt + 1, enqueued_at=eta)
            record = None
            if self._track:
                record = self._ser.encode_info(
                    self._info(
                        env, task, TaskState.RETRYING, started, finished,
                        error=_error_info(exc), next_attempt_at=eta,
                    )
                )  # fmt: skip
            self._submit(
                Completion(
                    delivery=d,
                    ack=True,
                    record=record,
                    record_task_id=env.id,
                    record_ttl=self._ttl,
                    reschedule=Reschedule(env.queue, env.id, self._ser.encode_envelope(nxt), eta),
                )
            )
            self.metrics.inc(env.queue, "retried")
            logger.warning(
                "task failed (%s: %s); retry in %.2fs",
                type(exc).__name__,
                exc,
                delay,
                extra=extra,
            )
            if self._promote_wake is not None and delay <= self.schedule_poll_interval:
                self._promote_wake.set()
            return
        reason = "max attempts exceeded" if retryable else "non-retryable error"
        logger.error(
            "task failed permanently (%s): %s: %s", reason, type(exc).__name__, exc, extra=extra
        )
        self._terminal(d, env, task, reason, exc, finished, started)

    def _terminal(
        self,
        d: Delivery,
        env: Envelope,
        task: Task[Any, Any] | None,
        reason: str,
        exc: BaseException | None,
        finished: float,
        started: float | None = None,
    ) -> None:
        error = (
            _error_info(exc) if exc is not None else ErrorInfo(type="BlitzQError", message=reason)
        )
        dead = task is None or task.opts.dead_letter
        store = task.opts.store_result if task is not None else self.app.store_results
        state = TaskState.DEAD_LETTERED if dead else TaskState.FAILED
        record = None
        if store:
            record = self._ser.encode_info(
                self._info(env, task, state, started, finished, error=error)
            )
        dlr = None
        if dead:
            self.metrics.inc(env.queue, "dead_lettered")
            dlr = DeadLetterRequest(
                env.id,
                env.queue,
                self._ser.encode_dead(
                    DeadLetter(
                        id=env.id,
                        queue=env.queue,
                        task=env.task,
                        reason=reason,
                        failed_at=finished,
                        attempt=env.attempt,
                        error=error,
                        message=d.data,
                    )
                ),
                self.app.dead_letter_max,
            )
        else:
            self.metrics.inc(env.queue, "failed")
        self._submit(
            Completion(
                delivery=d,
                ack=True,
                record=record,
                record_task_id=env.id,
                record_ttl=self._ttl,
                dead_letter=dlr,
            )
        )

    def _dead_letter_raw(self, d: Delivery, reason: str, exc: BaseException) -> None:
        tid = f"malformed-{uuid.uuid4().hex}"
        self._submit(
            Completion(
                delivery=d,
                ack=True,
                dead_letter=DeadLetterRequest(
                    tid,
                    d.queue,
                    self._ser.encode_dead(
                        DeadLetter(
                            id=tid,
                            queue=d.queue,
                            task="",
                            reason=reason,
                            failed_at=time.time(),
                            error=_error_info(exc, with_traceback=False),
                            message=d.data,
                        )
                    ),
                    self.app.dead_letter_max,
                ),
            )
        )

    def _cancelled(self, d: Delivery, env: Envelope) -> None:
        self.metrics.inc(env.queue, "cancelled")
        record = None
        if self.app.store_results or self._track:
            record = self._ser.encode_info(
                self._info(env, self.app.tasks.get(env.task), TaskState.CANCELLED,
                           finished=time.time())
            )  # fmt: skip
        self._submit(
            Completion(
                delivery=d, ack=True, record=record, record_task_id=env.id, record_ttl=self._ttl
            )
        )
        logger.info("task cancelled", extra={"task_id": env.id, "task": env.task})

    def _defer_rate_limited(self, d: Delivery, env: Envelope, wait: float) -> None:
        """Put a task that is over its rate limit back on the schedule.

        Not a retry: ``attempt`` is unchanged and nothing is recorded as a
        failure, since the task never ran.
        """
        eta = time.time() + wait
        nxt = msgspec.structs.replace(env, enqueued_at=eta)
        self.metrics.inc(env.queue, "rate_limited")
        self._submit(
            Completion(
                delivery=d,
                ack=True,
                reschedule=Reschedule(env.queue, env.id, self._ser.encode_envelope(nxt), eta),
            )
        )
        if self._promote_wake is not None and wait <= self.schedule_poll_interval:
            self._promote_wake.set()

    # -- background loops ----------------------------------------------------------
    async def _promote_loop(self) -> None:
        assert self._promote_wake is not None
        wake = self._promote_wake
        backoff = 0.1
        while not self._stopping:
            try:
                n, nxt = await self.broker.promote_due(time.time(), 1000)
                backoff = 0.1
            except Exception:
                logger.warning("promoting scheduled tasks failed", exc_info=True)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 5.0)
                continue
            if n >= 1000:
                continue
            wait = self.schedule_poll_interval
            if nxt is not None:
                wait = min(wait, max(0.0, nxt - time.time()))
            wake.clear()
            if wait > 0:
                try:
                    await asyncio.wait_for(wake.wait(), wait)
                except TimeoutError:
                    pass

    async def _revocation_loop(self) -> None:
        while not self._stopping:
            try:
                revoked = await self.broker.revoked()
                self._revoked = revoked
                for tid in revoked & self._running.keys():
                    t = self._running.get(tid)
                    if t is not None and tid not in self._cancel_reason:
                        self._cancel_reason[tid] = "revoked"
                        t.cancel()
            except Exception:
                logger.warning("revocation sync failed", exc_info=True)
            await asyncio.sleep(self.revocation_interval)

    def info(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "pid": os.getpid(),
            "host": socket.gethostname(),
            "queues": self.queues,
            "concurrency": self.concurrency,
            "running": self.global_limit.used,
            "processed": self.processed,
            "started_at": self._started_at,
            "seen_at": time.time(),
            "guarantee": self.broker.guarantee,
            "metrics": self.metrics.snapshot(),
        }

    async def _maintenance_loop(self) -> None:
        # Worker info (liveness + metrics for `blitzq queue stats`) is refreshed
        # every `stats_interval`; leases are renewed every `heartbeat_interval`.
        tick = min(self.heartbeat_interval, self.stats_interval)
        ttl = max(3, int(tick * 3))
        broker = self.broker
        next_lease = 0.0
        backoff = 0.1
        while not self._stopping:
            try:
                await broker.register_worker(self.id, msgspec.msgpack.encode(self.info()), ttl)
                now = time.monotonic()
                if broker.supports_recovery and now >= next_lease:
                    next_lease = now + self.heartbeat_interval
                    await self._renew_leases()
                    await self._recover()
            except Exception:
                # Retry quickly (not after the full `tick`) so a transient Redis
                # blip - a dropped connection, a brief timeout - doesn't leave
                # lease renewal and abandoned-message recovery stalled for
                # seconds while everything else on the worker has already
                # reconnected.
                logger.warning("worker maintenance failed", exc_info=True)
                await asyncio.sleep(min(backoff, tick))
                backoff = min(backoff * 2, tick)
                continue
            backoff = 0.1
            await asyncio.sleep(tick)

    async def _renew_leases(self) -> None:
        for queue, leases in self._leases.items():
            if not leases:
                continue
            lost = await self.broker.heartbeat(queue, list(leases.values()), self.id)
            for d in lost:
                logger.warning(
                    "lease lost; message may be executed again by another worker",
                    extra={"queue": queue},
                )
                leases.pop(d.receipt, None)

    async def _recover(self) -> None:
        for base in self.queues:
            qlim, glim = self.queue_limits[base], self.global_limit
            for lvl in PRIORITY_LEVELS:
                physical = physical_queue(base, lvl)
                n = min(qlim.free, glim.free, self.batch_size)
                if n <= 0 or self._stopping:
                    continue
                qlim.take(n)
                glim.take(n)
                try:
                    batch = await self.broker.recover(
                        physical, self.id, self.app.visibility_timeout, n
                    )
                except Exception:
                    qlim.release(n)
                    glim.release(n)
                    raise
                if len(batch) < n:
                    qlim.release(n - len(batch))
                    glim.release(n - len(batch))
                if batch:
                    self.metrics.inc(physical, "recovered", len(batch))
                    logger.warning(
                        "recovered %d abandoned messages", len(batch), extra={"queue": physical}
                    )
                for d in batch:
                    self._spawn(d, base)


def _error_info(exc: BaseException, with_traceback: bool = True) -> ErrorInfo:
    tb = None
    if with_traceback and exc.__traceback__ is not None:
        tb = "".join(traceback.format_exception(exc))[-_TRACEBACK_LIMIT:]
    return ErrorInfo(type=type(exc).__name__, message=str(exc)[:2000], traceback=tb)


def run_worker(worker: Worker) -> None:
    """Run ``worker`` in a new event loop with SIGINT/SIGTERM graceful shutdown.

    A second signal skips waiting for running tasks.
    """

    async def main() -> None:
        loop = asyncio.get_running_loop()
        hits = 0

        def on_signal() -> None:
            nonlocal hits
            hits += 1
            if hits == 1:
                logger.info("shutdown requested; finishing running tasks")
            else:
                logger.warning("second signal: shutting down immediately")
                worker.shutdown_timeout = 0
            worker.stop()

        sigs = [signal.SIGINT, signal.SIGTERM]
        if hasattr(signal, "SIGBREAK"):
            sigs.append(signal.SIGBREAK)
        for sig in sigs:
            try:
                loop.add_signal_handler(sig, on_signal)
            except (NotImplementedError, RuntimeError):
                if threading.current_thread() is threading.main_thread():
                    signal.signal(sig, lambda *_: loop.call_soon_threadsafe(on_signal))
        try:
            await worker.run()
        finally:
            await worker.app.close()

    asyncio.run(main())


logging.getLogger("blitzq").addHandler(logging.NullHandler())
