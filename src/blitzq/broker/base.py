"""The broker contract.

The worker, client and scheduler talk to brokers only through this interface;
all backend-specific commands live in the concrete implementations. Payloads
cross this boundary as opaque ``bytes`` produced by the serializer, so brokers
never need to understand task envelopes.

A broker covers three concerns:

* **Queues** - publish, fetch, acknowledge, requeue and recover runnable work.
* **Schedule** - hold delayed work (including retries) until it is due, then
  atomically promote it into its queue.
* **Records** - optional task state/results, dead letters, revocations,
  periodic-task coordination and worker registrations.

Every method is a coroutine and must be called from the event loop the broker
is used on. Implementations must be safe for concurrent use by many
coroutines.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, ClassVar, Literal

DeliveryGuarantee = Literal["at-most-once", "at-least-once", "ephemeral"]


@dataclass(slots=True)
class Delivery:
    """A message handed to a worker.

    ``receipt`` is an opaque broker-specific token used to acknowledge the
    message (for example a stream entry id). ``delivery_count`` is how many
    times the broker has handed out this particular message (1 for the first
    delivery) when the broker tracks it, else 1.
    """

    queue: str
    data: bytes
    receipt: Any = None
    delivery_count: int = 1


@dataclass(slots=True)
class PublishRequest:
    queue: str
    task_id: str
    data: bytes
    #: Absolute wall-clock time (epoch seconds) at which the task becomes runnable.
    eta: float | None = None
    #: Optional encoded task record written together with the message.
    record: bytes | None = None
    record_ttl: int | None = None


@dataclass(slots=True)
class Reschedule:
    queue: str
    task_id: str
    data: bytes
    eta: float


@dataclass(slots=True)
class DeadLetterRequest:
    task_id: str
    queue: str
    data: bytes  # encoded DeadLetter record
    max_entries: int = 0  # 0 = unbounded


@dataclass(slots=True)
class Completion:
    """Everything that must happen when a worker finishes with a delivery.

    Brokers apply all effects of one ``Completion`` atomically where the
    backend supports it (Redis: MULTI/EXEC), so a retry is never both
    acknowledged and lost, and a dead-lettered task is never left pending.
    """

    delivery: Delivery | None = None
    #: Acknowledge (remove) the delivery.
    ack: bool = True
    #: Encoded task record to store, with TTL in seconds.
    record: bytes | None = None
    record_task_id: str | None = None
    record_ttl: int | None = None
    #: Put the task back into the schedule (retry).
    reschedule: Reschedule | None = None
    #: Move the task to the dead-letter store.
    dead_letter: DeadLetterRequest | None = None
    #: Publish ``delivery.data`` back to the front of its queue (graceful shutdown).
    requeue: bool = False


@dataclass(slots=True)
class QueueStats:
    name: str
    #: Messages waiting to be delivered.
    waiting: int = 0
    #: Messages delivered but not yet acknowledged (reliable brokers only).
    in_progress: int | None = None
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ScheduledEntry:
    task_id: str
    queue: str
    eta: float
    data: bytes


class Broker(ABC):
    """Abstract broker. See module docstring."""

    #: Delivery guarantee offered for messages in queues.
    guarantee: ClassVar[DeliveryGuarantee]
    #: Whether in-flight messages of crashed workers are recovered.
    supports_recovery: ClassVar[bool] = False

    # -- lifecycle -----------------------------------------------------------------
    async def connect(self) -> None:  # noqa: B027 - optional hook
        """Open connections eagerly. Brokers may also connect lazily."""

    @abstractmethod
    async def close(self) -> None: ...

    @abstractmethod
    def clone(self) -> Broker:
        """Return a new, unconnected broker with identical configuration.

        Used to give a separate event loop (such as the synchronous API's
        background thread) its own connections.
        """

    async def ping(self) -> bool:
        return True

    # -- queues --------------------------------------------------------------------
    @abstractmethod
    async def publish(self, requests: Sequence[PublishRequest]) -> None:
        """Publish messages; those with ``eta`` in the future go to the schedule."""

    async def prepare_consumer(self, queues: Sequence[str], consumer: str) -> None:  # noqa: B027
        """Prepare broker-side structures before fetching (e.g. consumer groups)."""

    @abstractmethod
    async def fetch(self, queue: str, count: int, timeout: float, consumer: str) -> list[Delivery]:
        """Return up to ``count`` messages from ``queue``.

        ``timeout == 0`` means do not block. Otherwise block up to ``timeout``
        seconds waiting for at least one message.
        """

    @abstractmethod
    async def complete(self, completions: Sequence[Completion]) -> None:
        """Apply a batch of completions (acks, records, retries, dead letters)."""

    async def heartbeat(
        self, queue: str, deliveries: Sequence[Delivery], consumer: str
    ) -> list[Delivery]:
        """Extend the lease on in-flight deliveries.

        Returns the deliveries whose ownership was lost (another consumer
        recovered them). Brokers without leases return an empty list.
        """
        return []

    async def recover(
        self, queue: str, consumer: str, idle_timeout: float, count: int
    ) -> list[Delivery]:
        """Claim messages abandoned by other consumers for longer than ``idle_timeout``."""
        return []

    # -- schedule ------------------------------------------------------------------
    @abstractmethod
    async def promote_due(self, now: float, limit: int) -> tuple[int, float | None]:
        """Move due scheduled messages into their queues.

        Returns ``(promoted_count, next_eta)`` where ``next_eta`` is the due
        time of the earliest remaining scheduled message, if any. Must be safe
        to call concurrently from many processes.
        """

    @abstractmethod
    async def scheduled(self, limit: int = 100, offset: int = 0) -> list[ScheduledEntry]: ...

    @abstractmethod
    async def scheduled_count(self) -> int: ...

    @abstractmethod
    async def is_scheduled(self, task_id: str) -> float | None:
        """Return the ETA if ``task_id`` is currently scheduled."""

    # -- cancellation --------------------------------------------------------------
    @abstractmethod
    async def cancel(self, task_id: str, revoke_ttl: int) -> bool:
        """Cancel a task.

        Returns ``True`` if the task was scheduled and has been removed
        (cancellation is certain). Otherwise records a revocation that workers
        observe on their next revocation sync and returns ``False``.
        """

    @abstractmethod
    async def revoked(self) -> set[str]:
        """Currently active revocations."""

    # -- records -------------------------------------------------------------------
    @abstractmethod
    async def get_record(self, task_id: str) -> bytes | None: ...

    @abstractmethod
    async def set_record(self, task_id: str, data: bytes, ttl: int | None) -> None: ...

    # -- dead letters --------------------------------------------------------------
    @abstractmethod
    async def dead_letters(self, limit: int = 100, offset: int = 0) -> list[bytes]:
        """Dead-letter records, newest first."""

    @abstractmethod
    async def dead_letter_count(self) -> int: ...

    @abstractmethod
    async def get_dead_letter(self, task_id: str) -> bytes | None: ...

    @abstractmethod
    async def replay_dead_letter(self, task_id: str, queue: str, data: bytes) -> bool:
        """Atomically remove a dead letter, delete the task's stale record and
        publish ``data`` to ``queue``.

        Returns ``False`` if the dead letter no longer exists (for example it
        was replayed concurrently), in which case nothing is published.
        """

    @abstractmethod
    async def purge_dead_letters(self) -> int: ...

    # -- periodic coordination -----------------------------------------------------
    @abstractmethod
    async def claim_periodic(
        self, name: str, occurrence: float, queue: str, data: bytes | None
    ) -> bool:
        """Atomically record ``occurrence`` as dispatched and publish ``data``.

        Succeeds only if no occurrence ``>= occurrence`` was recorded before,
        so any number of scheduler instances dispatch each occurrence at most
        once. ``data=None`` records the occurrence without publishing (used by
        the ``skip`` missed-run policy).
        """

    @abstractmethod
    async def periodic_last(self) -> dict[str, float]: ...

    # -- inspection ----------------------------------------------------------------
    @abstractmethod
    async def queue_stats(self, queues: Sequence[str]) -> list[QueueStats]: ...

    @abstractmethod
    async def known_queues(self) -> set[str]: ...

    @abstractmethod
    async def register_worker(self, worker_id: str, info: bytes, ttl: int) -> None: ...

    @abstractmethod
    async def unregister_worker(self, worker_id: str) -> None: ...

    @abstractmethod
    async def workers(self) -> dict[str, bytes]: ...

    @abstractmethod
    async def purge_queue(self, queue: str) -> int: ...
