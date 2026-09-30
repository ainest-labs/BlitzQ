"""Task definitions and enqueueing."""

from __future__ import annotations

import inspect
import time
import uuid
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Generic, Literal, ParamSpec, TypeVar

from .broker.base import PublishRequest
from .context import current_task
from .exceptions import ConfigurationError
from .results import TaskHandle
from .retries import RetryPolicy
from .serialization import Envelope
from .state import TaskInfo, TaskState

if TYPE_CHECKING:
    from .client import Queue

P = ParamSpec("P")
R = TypeVar("R")

Executor = Literal["async", "thread", "process"]


@dataclass(frozen=True, slots=True)
class TaskOptions:
    name: str
    queue: str | None
    max_attempts: int
    retry_policy: RetryPolicy
    timeout: float | None
    executor: Executor
    store_result: bool
    dead_letter: bool


def new_task_id() -> str:
    return uuid.uuid4().hex


def _eta_from(delay: float | timedelta | None, eta: datetime | float | None) -> float | None:
    if delay is not None and eta is not None:
        raise ConfigurationError("pass either delay or eta, not both")
    if delay is not None:
        seconds = delay.total_seconds() if isinstance(delay, timedelta) else float(delay)
        return time.time() + seconds if seconds > 0 else None
    if eta is not None:
        if isinstance(eta, datetime):
            if eta.tzinfo is None:
                raise ConfigurationError("eta datetimes must be timezone-aware")
            return eta.timestamp()
        return float(eta)
    return None


@dataclass(frozen=True, slots=True)
class CallOptions:
    queue: str | None = None
    eta: float | None = None
    task_id: str | None = None
    correlation_id: str | None = None
    headers: Mapping[str, str] | None = None
    timeout: float | None = None


class Task(Generic[P, R]):
    """A function registered with a :class:`~blitzq.Queue`.

    Calling the object runs the function directly in the current process.
    ``enqueue`` publishes it for a worker. For ``async def`` functions ``R`` is
    the awaited return type.
    """

    def __init__(self, app: Queue, fn: Callable[P, Any], options: TaskOptions) -> None:
        self.app = app
        self.fn = fn
        self.opts = options
        self.name = options.name
        self.is_async = inspect.iscoroutinefunction(fn)
        if options.executor == "async" and not self.is_async:
            raise ConfigurationError(
                f"task {self.name}: executor='async' requires an async function"
            )
        if options.executor != "async" and self.is_async:
            raise ConfigurationError(f"task {self.name}: async functions must use executor='async'")
        self._queue_cache: str | None = None
        self.__doc__ = fn.__doc__
        self.__wrapped__ = fn

    def __repr__(self) -> str:
        return f"<Task {self.name}>"

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Any:
        return self.fn(*args, **kwargs)

    @property
    def queue(self) -> str:
        q = self._queue_cache
        if q is None:
            q = self._queue_cache = self.app.router.resolve(self.name, self.opts.queue, None)
        return q

    # -- enqueueing ------------------------------------------------------------------
    def options(
        self,
        *,
        queue: str | None = None,
        delay: float | timedelta | None = None,
        eta: datetime | float | None = None,
        task_id: str | None = None,
        correlation_id: str | None = None,
        headers: Mapping[str, str] | None = None,
        timeout: float | None = None,
    ) -> BoundTask[P, R]:
        """Return a view of this task with per-call options applied.

        ``delay`` (seconds or timedelta) and ``eta`` (aware datetime or epoch
        seconds) schedule the task for later. ``task_id`` sets an explicit id;
        enqueueing the same id twice creates two messages unless the first is
        still scheduled, in which case its schedule entry is replaced.
        """
        return BoundTask(
            self,
            CallOptions(
                queue=queue,
                eta=_eta_from(delay, eta),
                task_id=task_id,
                correlation_id=correlation_id,
                headers=headers,
                timeout=timeout,
            ),
        )

    def _build(
        self, args: tuple[Any, ...], kwargs: dict[str, Any], call: CallOptions | None
    ) -> tuple[PublishRequest, TaskHandle[R]]:
        app = self.app
        now = time.time()
        if call is None:
            queue = self.queue
            task_id = new_task_id()
            eta = None
            correlation_id = None
            headers = None
            timeout = None
        else:
            queue = call.queue or self.queue
            task_id = call.task_id or new_task_id()
            eta = call.eta
            correlation_id = call.correlation_id
            headers = dict(call.headers) if call.headers else None
            timeout = call.timeout
        parent = current_task()
        if parent is not None:
            # Propagate correlation id and trace headers to child tasks.
            if correlation_id is None:
                correlation_id = parent.correlation_id
            if parent.headers:
                headers = {**parent.headers, **(headers or {})}
        env = Envelope(
            id=task_id,
            task=self.name,
            queue=queue,
            args=list(args),
            kwargs=kwargs,
            attempt=1,
            created_at=now,
            enqueued_at=eta if eta is not None and eta > now else now,
            correlation_id=correlation_id,
            headers=headers,
            timeout=timeout,
        )
        data = app.serializer.encode_envelope(env)
        record = None
        if app.track_state:
            record = app.serializer.encode_info(
                TaskInfo(
                    id=task_id,
                    state=TaskState.SCHEDULED
                    if eta is not None and eta > now
                    else TaskState.QUEUED,
                    task=self.name,
                    queue=queue,
                    attempt=0,
                    max_attempts=self.opts.max_attempts,
                    created_at=now,
                    enqueued_at=env.enqueued_at,
                    correlation_id=correlation_id,
                )
            )
        req = PublishRequest(queue, task_id, data, eta, record, app.result_ttl)
        return req, TaskHandle(task_id, app, queue)

    async def enqueue(self, *args: P.args, **kwargs: P.kwargs) -> TaskHandle[R]:
        """Publish the task. Non-blocking for the event loop; returns once Redis accepted it."""
        req, handle = self._build(args, kwargs, None)
        await self.app.broker.publish((req,))
        return handle

    def enqueue_sync(self, *args: P.args, **kwargs: P.kwargs) -> TaskHandle[R]:
        """Blocking variant of :meth:`enqueue` for synchronous code."""
        req, handle = self._build(args, kwargs, None)
        self.app._sync_publish([req])
        return handle

    async def enqueue_many(self, items: Iterable[tuple[Any, ...]]) -> list[TaskHandle[R]]:
        """Publish many calls in one pipelined round-trip.

        Each item is a tuple of positional arguments.
        """
        built = [self._build(tuple(args), {}, None) for args in items]
        if built:
            await self.app.broker.publish([r for r, _ in built])
        return [h for _, h in built]

    def enqueue_many_sync(self, items: Iterable[tuple[Any, ...]]) -> list[TaskHandle[R]]:
        built = [self._build(tuple(args), {}, None) for args in items]
        if built:
            self.app._sync_publish([r for r, _ in built])
        return [h for _, h in built]


class BoundTask(Generic[P, R]):
    """A task with per-call options, created by :meth:`Task.options`."""

    __slots__ = ("call", "task")

    def __init__(self, task: Task[P, R], call: CallOptions) -> None:
        self.task = task
        self.call = call

    async def enqueue(self, *args: P.args, **kwargs: P.kwargs) -> TaskHandle[R]:
        req, handle = self.task._build(args, kwargs, self.call)
        await self.task.app.broker.publish((req,))
        return handle

    def enqueue_sync(self, *args: P.args, **kwargs: P.kwargs) -> TaskHandle[R]:
        req, handle = self.task._build(args, kwargs, self.call)
        self.task.app._sync_publish([req])
        return handle

    def _build_many(self, items: Iterable[tuple[Any, ...]]) -> list[tuple[PublishRequest, Any]]:
        if self.call.task_id is not None:
            raise ConfigurationError("enqueue_many cannot be combined with an explicit task_id")
        return [self.task._build(tuple(args), {}, self.call) for args in items]

    async def enqueue_many(self, items: Iterable[tuple[Any, ...]]) -> list[TaskHandle[R]]:
        """Publish many calls with these options in one pipelined round-trip."""
        built = self._build_many(items)
        if built:
            await self.task.app.broker.publish([r for r, _ in built])
        return [h for _, h in built]

    def enqueue_many_sync(self, items: Iterable[tuple[Any, ...]]) -> list[TaskHandle[R]]:
        built = self._build_many(items)
        if built:
            self.task.app._sync_publish([r for r, _ in built])
        return [h for _, h in built]
