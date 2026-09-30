"""The application object: task registry, configuration and client operations."""

from __future__ import annotations

import asyncio
import atexit
import os
import time
import weakref
from collections.abc import Awaitable, Callable, Coroutine, Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Literal, ParamSpec, Protocol, TypeVar, overload

import msgspec

from ._portal import Portal
from .broker import Broker, redis_broker
from .broker.base import PublishRequest, QueueStats
from .exceptions import ConfigurationError, ResultTimeout
from .ratelimit import RateLimit, as_rate_limit
from .results import TaskHandle, unwrap_result
from .retries import DEFAULT_RETRY_POLICY, RetryPolicy
from .routing import Router, Routes
from .schedules import Schedule, as_schedule
from .serialization import DeadLetter, Serializer
from .state import TaskInfo, TaskState
from .task import Executor, Priority, Task, TaskOptions, new_task_id

P = ParamSpec("P")
R = TypeVar("R")
T = TypeVar("T")

MissedPolicy = Literal["skip", "run_once", "run_all"]
Hook = Callable[[], Any]
SyncWrapper = Callable[[Callable[[], Any]], Any]

DEFAULT_REDIS_URL = "redis://localhost:6379/0"


class _TaskDecorator(Protocol):
    @overload
    def __call__(self, fn: Callable[P, Coroutine[Any, Any, R]], /) -> Task[P, R]: ...
    @overload
    def __call__(self, fn: Callable[P, R], /) -> Task[P, R]: ...


@dataclass(slots=True)
class PeriodicTask:
    name: str
    task: Task[Any, Any]
    schedule: Schedule
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)
    missed: MissedPolicy = "run_once"
    queue: str | None = None


class Queue:
    """A BlitzQ application.

    ``Queue`` holds configuration and the task registry, and provides client
    operations (enqueue, results, inspection, cancellation). The same object
    is imported by web processes, which only publish, and by workers started
    with ``blitzq worker module:attribute``.

    Parameters
    ----------
    name:
        Default queue for tasks without an explicit queue or routing rule.
    redis_url:
        Redis connection URL (``rediss://`` for TLS). Defaults to the
        ``BLITZQ_REDIS_URL`` environment variable, then ``redis://localhost:6379/0``.
    mode:
        ``"reliable"`` (default; Redis Streams, at-least-once, crash recovery)
        or ``"fast"`` (Redis lists, at-most-once, lowest overhead).
    broker:
        A custom :class:`~blitzq.broker.base.Broker` instead of Redis.
    store_results:
        Store final state and return values (can be overridden per task).
    result_ttl:
        Seconds to keep task records.
    track_state:
        Also record ``queued``/``scheduled``/``running``/``retrying``
        transitions. Costs one extra write per transition.
    routes:
        Task-name glob patterns to queue names, or a callable.
    visibility_timeout:
        Reliable mode: seconds after which an unacknowledged message whose
        worker stopped renewing its lease is redelivered.
    max_deliveries:
        Reliable mode: dead-letter a message after this many deliveries
        (protects against messages that crash workers).
    """

    def __init__(
        self,
        name: str = "default",
        redis_url: str | None = None,
        *,
        mode: Literal["fast", "reliable"] = "reliable",
        broker: Broker | None = None,
        namespace: str = "blitzq",
        serializer: Serializer | None = None,
        store_results: bool = True,
        result_ttl: int = 24 * 3600,
        track_state: bool = False,
        routes: Routes = None,
        default_retries: int = 0,
        default_retry_policy: RetryPolicy | None = None,
        default_timeout: float | None = None,
        visibility_timeout: float = 60.0,
        max_deliveries: int = 5,
        dead_letter_max: int = 100_000,
        revoke_ttl: int = 3600,
        redis_options: dict[str, Any] | None = None,
    ) -> None:
        self.name = name
        self.mode = mode
        if broker is None:
            url = redis_url or os.environ.get("BLITZQ_REDIS_URL") or DEFAULT_REDIS_URL
            broker = redis_broker(
                url,
                mode,
                namespace=namespace,
                dead_letter_max=dead_letter_max,
                **(redis_options or {}),
            )
            self.redis_url: str | None = url
        else:
            self.redis_url = None
        self.broker: Broker = broker
        self.namespace = namespace
        self.serializer = serializer or Serializer()
        self.store_results = store_results
        self.result_ttl = result_ttl
        self.track_state = track_state
        self.router = Router(routes, name)
        self.default_retries = default_retries
        self.default_retry_policy = default_retry_policy or DEFAULT_RETRY_POLICY
        self.default_timeout = default_timeout
        if visibility_timeout <= 0:
            raise ConfigurationError("visibility_timeout must be positive")
        self.visibility_timeout = visibility_timeout
        self.max_deliveries = max_deliveries
        self.dead_letter_max = dead_letter_max
        self.revoke_ttl = revoke_ttl
        self.tasks: dict[str, Task[Any, Any]] = {}
        self.periodic_tasks: dict[str, PeriodicTask] = {}
        self.startup_hooks: list[Hook] = []
        self.shutdown_hooks: list[Hook] = []
        self.sync_wrappers: list[SyncWrapper] = []
        self._portal: Portal | None = None
        self._sync_broker: Broker | None = None

    def __repr__(self) -> str:
        return f"<Queue {self.name!r} mode={self.mode} tasks={len(self.tasks)}>"

    # -- registration --------------------------------------------------------------
    @overload
    def task(self, fn: Callable[P, Coroutine[Any, Any, R]], /) -> Task[P, R]: ...
    @overload
    def task(self, fn: Callable[P, R], /) -> Task[P, R]: ...
    @overload
    def task(
        self,
        fn: None = None,
        /,
        *,
        name: str | None = None,
        queue: str | None = None,
        retries: int | None = None,
        retry_policy: RetryPolicy | None = None,
        timeout: float | None = None,
        executor: Executor | None = None,
        store_result: bool | None = None,
        dead_letter: bool = True,
        priority: Priority = "normal",
        rate_limit: RateLimit | str | None = None,
    ) -> _TaskDecorator: ...

    def task(
        self,
        fn: Callable[..., Any] | None = None,
        /,
        *,
        name: str | None = None,
        queue: str | None = None,
        retries: int | None = None,
        retry_policy: RetryPolicy | None = None,
        timeout: float | None = None,
        executor: Executor | None = None,
        store_result: bool | None = None,
        dead_letter: bool = True,
        priority: Priority = "normal",
        rate_limit: RateLimit | str | None = None,
    ) -> Any:
        """Register a task.

        ``retries`` is the number of retries after the first attempt.
        ``executor`` defaults to ``"async"`` for coroutine functions and
        ``"thread"`` for regular functions; use ``"process"`` for CPU-bound
        functions (they must be importable module-level functions).
        ``dead_letter=False`` records terminal failures as ``failed`` instead
        of moving them to the dead-letter store.

        ``priority`` (``"high"``/``"normal"``/``"low"``) is this task's
        default priority within its queue; override per call with
        ``task.options(priority=...)``. Priority levels are physically
        separate broker queues that every worker checks in order (high,
        then normal, then low) while sharing the queue's overall concurrency
        - not a separate queue you need to remember to subscribe a worker to.
        See docs/architecture.md#task-priority.

        ``rate_limit`` caps how often this task *starts* execution, shared
        across every worker (a Redis-backed token bucket, not a per-process
        counter): ``"10/s"``, ``"100/m"`` or ``"1000/hour"``, or a
        :class:`~blitzq.RateLimit`. A task over its limit is not executed and
        not counted as a retry; it is rescheduled for when a slot should be
        free. See docs/architecture.md#rate-limiting.
        """

        def register(f: Callable[..., Any]) -> Task[Any, Any]:
            task_name = name or f"{f.__module__}.{f.__qualname__}"
            if task_name in self.tasks:
                raise ConfigurationError(f"task {task_name!r} is already registered")
            n_retries = self.default_retries if retries is None else retries
            if n_retries < 0:
                raise ConfigurationError("retries must be >= 0")
            is_async = asyncio.iscoroutinefunction(f)
            opts = TaskOptions(
                name=task_name,
                queue=queue,
                max_attempts=n_retries + 1,
                retry_policy=retry_policy or self.default_retry_policy,
                timeout=timeout if timeout is not None else self.default_timeout,
                executor=executor or ("async" if is_async else "thread"),
                store_result=self.store_results if store_result is None else store_result,
                dead_letter=dead_letter,
                priority=priority,
                rate_limit=as_rate_limit(rate_limit),
            )
            t: Task[Any, Any] = Task(self, f, opts)
            self.tasks[task_name] = t
            return t

        if fn is not None:
            return register(fn)
        return register

    def periodic(
        self,
        schedule: Schedule | str | float | timedelta,
        *,
        tz: str | None = None,
        args: Sequence[Any] = (),
        kwargs: dict[str, Any] | None = None,
        missed: MissedPolicy = "run_once",
        name: str | None = None,
        queue: str | None = None,
        **task_options: Any,
    ) -> Callable[[Callable[..., Any]], Task[Any, Any]]:
        """Register a periodic task (dispatched by ``blitzq scheduler``).

        ``schedule`` is a cron string (``"*/5 * * * *"``, evaluated in ``tz``),
        an interval in seconds or a ``timedelta``, or a :class:`Schedule`.
        ``missed`` controls what happens to occurrences missed while no
        scheduler was running: ``"run_once"`` dispatches only the latest,
        ``"run_all"`` dispatches each (up to 100), ``"skip"`` dispatches none.
        """
        sched = as_schedule(schedule, tz)

        def register(f: Callable[..., Any]) -> Task[Any, Any]:
            t = f if isinstance(f, Task) else self.task(name=name, queue=queue, **task_options)(f)
            if t.name in self.periodic_tasks:
                raise ConfigurationError(f"periodic task {t.name!r} is already registered")
            self.periodic_tasks[t.name] = PeriodicTask(
                t.name, t, sched, tuple(args), dict(kwargs or {}), missed, queue
            )
            return t

        return register

    def on_startup(self, fn: Hook) -> Hook:
        """Register a worker/scheduler startup hook (sync or async)."""
        self.startup_hooks.append(fn)
        return fn

    def on_shutdown(self, fn: Hook) -> Hook:
        """Register a worker/scheduler shutdown hook (sync or async)."""
        self.shutdown_hooks.append(fn)
        return fn

    def add_sync_wrapper(self, wrapper: SyncWrapper) -> None:
        """Wrap every sync-task call inside its executor thread.

        ``wrapper(call)`` must invoke ``call()`` and return its result. Used by
        integrations that need thread-local setup, such as Django connection
        management or a Flask application context.
        """
        self.sync_wrappers.append(wrapper)

    # -- lifecycle -----------------------------------------------------------------
    async def connect(self) -> None:
        """Eagerly connect the async broker (optional; connections are lazy)."""
        await self.broker.connect()

    async def close(self) -> None:
        """Close async broker connections. Safe to call more than once."""
        await self.broker.close()

    async def __aenter__(self) -> Queue:
        await self.connect()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    # -- synchronous plumbing ------------------------------------------------------
    def _portal_call(self, fn: Callable[..., Awaitable[T]], *args: Any, what: str) -> T:
        portal = self._portal
        if portal is None:
            portal = self._portal = Portal()
            _register_portal(self)

        async def run() -> T:
            broker = self._sync_broker
            if broker is None:
                broker = self._sync_broker = self.broker.clone()
            return await fn(broker, *args)

        return portal.call(run, what=what)

    def _sync_publish(self, reqs: list[PublishRequest]) -> None:
        async def go(broker: Broker) -> None:
            await broker.publish(reqs)

        self._portal_call(go, what="enqueue_sync()")

    def close_sync(self) -> None:
        """Close connections used by the synchronous API and stop its thread."""
        portal, self._portal = self._portal, None
        broker, self._sync_broker = self._sync_broker, None
        if portal is not None:
            portal.stop(broker.close if broker is not None else None)

    # -- enqueue by name -----------------------------------------------------------
    async def send(
        self,
        task_name: str,
        args: Sequence[Any] = (),
        kwargs: dict[str, Any] | None = None,
        **options: Any,
    ) -> TaskHandle[Any]:
        """Enqueue a task by name, without importing its code (producer-only apps)."""
        return await self._named(task_name).options(**options).enqueue(*args, **(kwargs or {}))

    def send_sync(
        self,
        task_name: str,
        args: Sequence[Any] = (),
        kwargs: dict[str, Any] | None = None,
        **options: Any,
    ) -> TaskHandle[Any]:
        return self._named(task_name).options(**options).enqueue_sync(*args, **(kwargs or {}))

    def _named(self, task_name: str) -> Task[Any, Any]:
        t = self.tasks.get(task_name)
        if t is not None:
            return t

        def _remote(*a: Any, **k: Any) -> Any:
            raise RuntimeError("remote task stub cannot be called locally")

        return Task(
            self,
            _remote,
            TaskOptions(
                task_name, None, 1, self.default_retry_policy, None, "thread",
                self.store_results, True, "normal", None,
            ),
        )  # fmt: skip

    # -- results and inspection ----------------------------------------------------
    async def _inspect(self, broker: Broker, task_id: str) -> TaskInfo | None:
        data = await broker.get_record(task_id)
        if data is not None:
            return self.serializer.decode_info(data)
        eta = await broker.is_scheduled(task_id)
        if eta is not None:
            return TaskInfo(id=task_id, state=TaskState.SCHEDULED, next_attempt_at=eta)
        dead = await broker.get_dead_letter(task_id)
        if dead is not None:
            d = self.serializer.decode_dead(dead)
            return TaskInfo(
                id=task_id,
                state=TaskState.DEAD_LETTERED,
                task=d.task,
                queue=d.queue,
                attempt=d.attempt,
                finished_at=d.failed_at,
                error=d.error,
            )
        return None

    async def inspect(self, task_id: str) -> TaskInfo | None:
        """Everything known about a task, or ``None``.

        ``None`` means the task is queued or running without state tracking,
        its record expired, or the id is unknown.
        """
        return await self._inspect(self.broker, task_id)

    def inspect_sync(self, task_id: str) -> TaskInfo | None:
        return self._portal_call(self._inspect, task_id, what="inspect_sync()")

    async def status(self, task_id: str) -> TaskState | None:
        info = await self.inspect(task_id)
        return info.state if info else None

    def status_sync(self, task_id: str) -> TaskState | None:
        info = self.inspect_sync(task_id)
        return info.state if info else None

    async def _get_result(self, broker: Broker, task_id: str, timeout: float | None) -> Any:
        deadline = None if timeout is None else time.monotonic() + timeout
        # Safety-net ceiling on each wait: brokers that push a notification
        # (Redis: pub/sub) return almost immediately once the task finishes;
        # this bounds how long a missed notification (e.g. a dropped pub/sub
        # message during a reconnect) can delay noticing a record that's
        # already there.
        max_wait = 5.0
        last: TaskInfo | None = None
        while True:
            data = await broker.get_record(task_id)
            if data is not None:
                last = self.serializer.decode_info(data)
                if last.state.is_final:
                    return unwrap_result(last)
            elif await broker.get_dead_letter(task_id) is not None:
                info = await self._inspect(broker, task_id)
                assert info is not None
                return unwrap_result(info)
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise ResultTimeout(task_id, timeout, last.state if last else None)
                wait = min(max_wait, remaining)
            else:
                wait = max_wait
            await broker.wait_for_record(task_id, wait)

    async def get_result(self, task_id: str, timeout: float | None = None) -> Any:
        """Wait for a task's final state and return its result.

        Waits on the broker's push notification when it has one (Redis:
        pub/sub on the task's record channel), falling back to a coarse
        poll otherwise. Requires result storage for the task. Raises
        ``TaskFailed`` for failed, dead-lettered or cancelled tasks and
        ``ResultTimeout`` after ``timeout`` seconds (``None`` waits
        indefinitely).
        """
        return await self._get_result(self.broker, task_id, timeout)

    def get_result_sync(self, task_id: str, timeout: float | None = None) -> Any:
        return self._portal_call(self._get_result, task_id, timeout, what="get_result_sync()")

    # -- cancellation / retry ------------------------------------------------------
    async def _cancel(self, broker: Broker, task_id: str) -> bool:
        removed = await broker.cancel(task_id, self.revoke_ttl)
        if removed and self.store_results:
            info = TaskInfo(id=task_id, state=TaskState.CANCELLED, finished_at=time.time())
            await broker.set_record(task_id, self.serializer.encode_info(info), self.result_ttl)
        return removed

    async def cancel(self, task_id: str) -> bool:
        """Cancel a task.

        Returns ``True`` when the task was still scheduled (delayed or waiting
        for a retry) and has been removed - cancellation is then certain.
        Otherwise a revocation is recorded and ``False`` is returned: workers
        skip the task if they receive it after their next revocation sync
        (about one second) and cancel it if it is a running ``async`` task.
        Running thread/process tasks cannot be interrupted, and a task that
        already finished is unaffected.
        """
        return await self._cancel(self.broker, task_id)

    def cancel_sync(self, task_id: str) -> bool:
        return self._portal_call(self._cancel, task_id, what="cancel_sync()")

    async def _retry(self, broker: Broker, task_id: str) -> bool:
        raw = await broker.get_dead_letter(task_id)
        if raw is None:
            return False
        dead = self.serializer.decode_dead(raw)
        env = self.serializer.decode_envelope(dead.message)
        now = time.time()
        env = msgspec.structs.replace(env, attempt=1, enqueued_at=now)
        return await broker.replay_dead_letter(
            task_id, env.queue, self.serializer.encode_envelope(env)
        )

    async def retry(self, task_id: str) -> bool:
        """Re-enqueue a dead-lettered task with a fresh attempt budget.

        Returns ``False`` if no dead letter with this id exists.
        """
        return await self._retry(self.broker, task_id)

    def retry_sync(self, task_id: str) -> bool:
        return self._portal_call(self._retry, task_id, what="retry_sync()")

    # -- dead letters / stats ------------------------------------------------------
    async def dead_letters(self, limit: int = 100, offset: int = 0) -> list[DeadLetter]:
        raw = await self.broker.dead_letters(limit, offset)
        return [self.serializer.decode_dead(r) for r in raw]

    async def queue_stats(self, queues: Sequence[str] | None = None) -> list[QueueStats]:
        if queues is None:
            names = set(await self.broker.known_queues())
            names.add(self.name)
            names.update(t.queue for t in self.tasks.values())
            queues = sorted(names)
        return await self.broker.queue_stats(queues)

    async def purge(self, queue: str | None = None) -> int:
        """Delete waiting messages from ``queue`` (default queue if omitted)."""
        return await self.broker.purge_queue(queue or self.name)

    def new_task_id(self) -> str:
        return new_task_id()


_portal_apps: weakref.WeakSet[Queue] = weakref.WeakSet()


def _register_portal(app: Queue) -> None:
    if not _portal_apps:
        atexit.register(_close_portals)
    _portal_apps.add(app)


def _close_portals() -> None:
    for app in list(_portal_apps):
        try:
            app.close_sync()
        except Exception:
            pass
