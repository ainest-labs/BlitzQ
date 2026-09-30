"""Per-execution task context, available to running task code."""

from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class TaskContext:
    """Metadata about the currently executing task.

    Available through :func:`current_task` inside async tasks and tasks run on
    the thread executor. Not available inside the process executor.
    """

    id: str
    name: str
    queue: str
    attempt: int
    max_attempts: int
    correlation_id: str | None = None
    headers: dict[str, str] = field(default_factory=dict)
    enqueued_at: float = 0.0
    worker: str | None = None

    @property
    def retries(self) -> int:
        return self.attempt - 1

    @property
    def is_last_attempt(self) -> bool:
        return self.attempt >= self.max_attempts


_current: ContextVar[TaskContext | None] = ContextVar("blitzq_current_task", default=None)


def current_task() -> TaskContext | None:
    """Return the context of the task being executed, or ``None`` outside a task."""
    return _current.get()
