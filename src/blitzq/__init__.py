"""BlitzQ - a high-performance, framework-agnostic asyncio task queue.

Public API::

    from blitzq import Queue, RetryPolicy, Retry, current_task

See https://github.com/ainest-labs/BlitzQ for documentation.
"""

from ._version import __version__
from .client import Queue
from .context import TaskContext, current_task
from .deadletters import BulkResult, DeadLetterFilter
from .exceptions import (
    BlitzQError,
    ConfigurationError,
    MessageTooLarge,
    ResultTimeout,
    Retry,
    SerializationError,
    TaskFailed,
    TaskTimeout,
)
from .ratelimit import RateLimit
from .results import TaskHandle
from .retries import RetryPolicy
from .schedules import Cron, Every
from .serialization import Serializer
from .state import TaskInfo, TaskState
from .task import Priority, Task
from .worker import Worker

__all__ = [
    "BlitzQError",
    "BulkResult",
    "ConfigurationError",
    "Cron",
    "DeadLetterFilter",
    "Every",
    "MessageTooLarge",
    "Priority",
    "Queue",
    "RateLimit",
    "ResultTimeout",
    "Retry",
    "RetryPolicy",
    "SerializationError",
    "Serializer",
    "Task",
    "TaskContext",
    "TaskFailed",
    "TaskHandle",
    "TaskInfo",
    "TaskState",
    "TaskTimeout",
    "Worker",
    "__version__",
    "current_task",
]
