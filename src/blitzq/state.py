"""Task states and the stored task record."""

from __future__ import annotations

from enum import StrEnum
from typing import Any

import msgspec


class TaskState(StrEnum):
    SCHEDULED = "scheduled"
    QUEUED = "queued"
    RUNNING = "running"
    RETRYING = "retrying"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    DEAD_LETTERED = "dead_lettered"
    CANCELLED = "cancelled"

    @property
    def is_final(self) -> bool:
        return self in _FINAL


_FINAL = frozenset(
    {TaskState.SUCCEEDED, TaskState.FAILED, TaskState.DEAD_LETTERED, TaskState.CANCELLED}
)


class ErrorInfo(msgspec.Struct, omit_defaults=True):
    type: str
    message: str
    traceback: str | None = None


class TaskInfo(msgspec.Struct, omit_defaults=True):
    """A snapshot of what BlitzQ knows about a task.

    Records are written by producers (``queued``/``scheduled`` when state
    tracking is enabled) and by workers (``running`` when tracking is enabled,
    and the final state when results are stored). Readers may observe a
    slightly stale state during concurrent transitions; see
    ``docs/delivery_guarantees.md``.
    """

    id: str
    state: TaskState
    task: str = ""
    queue: str = ""
    attempt: int = 0
    max_attempts: int = 0
    created_at: float | None = None
    enqueued_at: float | None = None
    started_at: float | None = None
    finished_at: float | None = None
    result: Any = None
    error: ErrorInfo | None = None
    correlation_id: str | None = None
    worker: str | None = None
    next_attempt_at: float | None = None

    @property
    def duration(self) -> float | None:
        """Execution time of the last attempt in seconds, if known."""
        if self.started_at is None or self.finished_at is None:
            return None
        return self.finished_at - self.started_at

    @property
    def retries(self) -> int:
        """Number of retries performed so far (attempts after the first)."""
        return max(0, self.attempt - 1)
