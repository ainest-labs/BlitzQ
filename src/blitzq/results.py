"""Handles to enqueued tasks and result retrieval."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, TypeVar

from .state import TaskInfo, TaskState

if TYPE_CHECKING:
    from .client import Queue

R = TypeVar("R")


class TaskHandle(Generic[R]):
    """Reference to an enqueued task.

    Async methods (``result``, ``info``, ``status``, ``cancel``) must be awaited
    inside an event loop; ``*_sync`` variants block the calling thread and must
    not be used inside a running event loop.
    """

    __slots__ = ("app", "id", "queue")

    def __init__(self, task_id: str, app: Queue, queue: str) -> None:
        self.id = task_id
        self.app = app
        self.queue = queue

    def __repr__(self) -> str:
        return f"<TaskHandle id={self.id} queue={self.queue}>"

    async def result(self, timeout: float | None = None) -> R:
        """Wait for the task to finish and return its result.

        Raises :class:`~blitzq.exceptions.TaskFailed` if it failed, was
        dead-lettered or cancelled, and :class:`~blitzq.exceptions.ResultTimeout`
        if no final state is visible within ``timeout`` seconds.
        """
        value: R = await self.app.get_result(self.id, timeout=timeout)
        return value

    def result_sync(self, timeout: float | None = None) -> R:
        value: R = self.app.get_result_sync(self.id, timeout=timeout)
        return value

    async def info(self) -> TaskInfo | None:
        return await self.app.inspect(self.id)

    async def status(self) -> TaskState | None:
        return await self.app.status(self.id)

    async def cancel(self) -> bool:
        return await self.app.cancel(self.id)

    def cancel_sync(self) -> bool:
        return self.app.cancel_sync(self.id)

    def status_sync(self) -> TaskState | None:
        return self.app.status_sync(self.id)


def unwrap_result(info: TaskInfo) -> Any:
    """Return ``info.result`` or raise ``TaskFailed`` for unsuccessful final states."""
    from .exceptions import TaskFailed

    if info.state is TaskState.SUCCEEDED:
        return info.result
    err = info.error
    raise TaskFailed(
        info.id,
        info.state.value,
        err.type if err else None,
        err.message if err else None,
        err.traceback if err else None,
    )
