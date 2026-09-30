"""Exception types raised by BlitzQ or used to signal control flow from tasks."""

from __future__ import annotations

from typing import Any


class BlitzQError(Exception):
    """Base class for all BlitzQ errors."""


class ConfigurationError(BlitzQError):
    """Invalid configuration or API misuse."""


class SerializationError(BlitzQError):
    """A value could not be encoded or decoded with the configured serializer."""


class MessageTooLarge(SerializationError):
    """An encoded message exceeds the configured ``max_message_size``."""


class UnknownTask(BlitzQError):
    """A worker received a message for a task name that is not registered."""


class TaskTimeout(BlitzQError, TimeoutError):
    """A task exceeded its execution time limit.

    For ``async`` tasks the coroutine is cancelled. For thread and process
    executors the worker stops waiting, but the underlying call keeps running
    until it returns (Python cannot safely kill a thread).
    """


class Retry(BlitzQError):
    """Raise from inside a task to request another attempt.

    ``delay`` overrides the retry policy's computed backoff (seconds). The
    request still counts toward the task's maximum attempts, so explicit
    retries cannot loop forever.
    """

    def __init__(self, delay: float | None = None, reason: str | None = None) -> None:
        super().__init__(reason or "retry requested")
        self.delay = delay
        self.reason = reason


class TaskFailed(BlitzQError):
    """Raised by result retrieval when the task finished unsuccessfully."""

    def __init__(
        self,
        task_id: str,
        state: str,
        error_type: str | None = None,
        error_message: str | None = None,
        traceback: str | None = None,
    ) -> None:
        detail = f"{error_type}: {error_message}" if error_type else state
        super().__init__(f"task {task_id} {state}: {detail}")
        self.task_id = task_id
        self.state = state
        self.error_type = error_type
        self.error_message = error_message
        self.traceback = traceback


class ResultTimeout(BlitzQError, TimeoutError):
    """No final result became available within the requested wait time."""

    def __init__(self, task_id: str, timeout: float | None, last_state: Any = None) -> None:
        super().__init__(f"no result for task {task_id} within {timeout}s (state={last_state})")
        self.task_id = task_id
        self.timeout = timeout
        self.last_state = last_state


class ResultNotStored(BlitzQError):
    """The task's result is not available (results disabled, expired, or unknown id)."""
