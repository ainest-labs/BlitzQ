"""Dead-letter inspection and bulk operations.

Every operation works the same way: take a snapshot of the matching ids from the
broker (a cheap range query, restricted by failure time when asked), then fetch
and filter the records in bounded chunks. Replaying or deleting never shifts the
snapshot, so a bulk run cannot skip or repeat entries however many it removes.

Replaying one entry is atomic in the broker (it is removed from the dead-letter
store and published in one step), so two operators running the same bulk
requeue, or a requeue racing a purge, can never publish an entry twice.
"""

from __future__ import annotations

import asyncio
import fnmatch
import time
from collections import Counter
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

import msgspec

from .exceptions import ConfigurationError, SerializationError
from .serialization import DeadLetter, Envelope

if TYPE_CHECKING:
    from .broker.base import Broker
    from .client import Queue

CHUNK = 200
REPLAY_BATCH = 100
_PRIORITY_SUFFIXES = (":high", ":low")
_FIELD_GROUPS = ("task", "queue", "reason", "error_type", "rate_key")


def _epoch(value: float | datetime | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ConfigurationError("failed_after/failed_before datetimes must be timezone-aware")
        return value.timestamp()
    return float(value)


def _base_queue(name: str) -> str:
    for suffix in _PRIORITY_SUFFIXES:
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return name


@dataclass(frozen=True, slots=True)
class DeadLetterFilter:
    """Selects dead letters. Every field that is set must match.

    ``queue`` matches the queue and its priority levels. ``task`` is an exact
    name or a glob (``"billing.*"``). ``error_type`` is the exception class name
    and ``error_contains`` a case-insensitive substring of its message.
    ``headers`` (every pair must match), ``rate_key`` and ``correlation_id`` are
    read from the original message, which is how you select by tenant, branch or
    country: set a header such as ``headers={"country": "IN"}`` when enqueueing.
    ``failed_after`` / ``failed_before`` take epoch seconds or aware datetimes.
    """

    queue: str | None = None
    task: str | None = None
    reason: str | None = None
    error_type: str | None = None
    error_contains: str | None = None
    headers: Mapping[str, str] | None = None
    rate_key: str | None = None
    correlation_id: str | None = None
    failed_after: float | datetime | None = None
    failed_before: float | datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "failed_after", _epoch(self.failed_after))
        object.__setattr__(self, "failed_before", _epoch(self.failed_before))

    @property
    def is_empty(self) -> bool:
        return all(getattr(self, f) is None for f in self.__slots__)

    @property
    def _needs_envelope(self) -> bool:
        return bool(self.headers) or self.rate_key is not None or self.correlation_id is not None

    def matches(self, dead: DeadLetter, serializer: Any) -> bool:
        if self.queue is not None and self.queue not in (dead.queue, _base_queue(dead.queue)):
            return False
        if self.task is not None and not fnmatch.fnmatchcase(dead.task, self.task):
            return False
        if self.reason is not None and dead.reason != self.reason:
            return False
        if self.error_type is not None and (
            dead.error is None or dead.error.type != self.error_type
        ):
            return False
        if self.error_contains is not None:
            haystack = f"{dead.error.message if dead.error else ''}\n{dead.reason}".lower()
            if self.error_contains.lower() not in haystack:
                return False
        after, before = self.failed_after, self.failed_before
        if after is not None and dead.failed_at < after:  # type: ignore[operator]
            return False
        if before is not None and dead.failed_at > before:  # type: ignore[operator]
            return False
        if self._needs_envelope:
            env = _envelope(dead, serializer)
            if env is None:
                return False
            if self.headers and any(
                (env.headers or {}).get(k) != v for k, v in self.headers.items()
            ):
                return False
            if self.rate_key is not None and env.rate_key != self.rate_key:
                return False
            if self.correlation_id is not None and env.correlation_id != self.correlation_id:
                return False
        return True


@dataclass(slots=True)
class BulkResult:
    """Outcome of a bulk requeue.

    ``matched`` entries met the filter (capped by ``limit``). Of those,
    ``requeued`` were published, ``skipped`` had already been replayed or purged
    by someone else, and ``unreplayable`` had a message that can no longer be
    decoded. A dry run only fills ``matched`` and ``ids``.
    """

    matched: int = 0
    requeued: int = 0
    skipped: int = 0
    unreplayable: int = 0
    dry_run: bool = False
    ids: list[str] = field(default_factory=list)


def _envelope(dead: DeadLetter, serializer: Any) -> Envelope | None:
    try:
        env: Envelope = serializer.decode_envelope(dead.message)
    except SerializationError:
        return None
    return env


async def scan(
    app: Queue, broker: Broker, filt: DeadLetterFilter, *, oldest_first: bool = False
) -> AsyncIterator[DeadLetter]:
    ids = await broker.dead_letter_ids(
        after=filt.failed_after,  # type: ignore[arg-type]
        before=filt.failed_before,  # type: ignore[arg-type]
        oldest_first=oldest_first,
    )
    ser = app.serializer
    for start in range(0, len(ids), CHUNK):
        for raw in await broker.get_dead_letters(ids[start : start + CHUNK]):
            if raw is None:
                continue  # removed since the snapshot
            try:
                dead = ser.decode_dead(raw)
            except SerializationError:
                continue
            if filt.matches(dead, ser):
                yield dead


async def collect(
    app: Queue,
    broker: Broker,
    filt: DeadLetterFilter,
    *,
    limit: int | None = None,
    offset: int = 0,
    oldest_first: bool = False,
) -> list[DeadLetter]:
    out: list[DeadLetter] = []
    skipped = 0
    async for dead in scan(app, broker, filt, oldest_first=oldest_first):
        if skipped < offset:
            skipped += 1
            continue
        out.append(dead)
        if limit is not None and len(out) >= limit:
            break
    return out


async def _replay_one(app: Queue, broker: Broker, dead: DeadLetter) -> str:
    ser = app.serializer
    env = _envelope(dead, ser)
    if env is None:
        return "unreplayable"
    env = msgspec.structs.replace(env, attempt=1, enqueued_at=time.time())
    ok = await broker.replay_dead_letter(dead.id, env.queue, ser.encode_envelope(env))
    return "requeued" if ok else "skipped"


async def retry_matching(
    app: Queue,
    broker: Broker,
    filt: DeadLetterFilter,
    *,
    limit: int | None = None,
    rate: float | None = None,
    dry_run: bool = False,
) -> BulkResult:
    """Re-enqueue matching dead letters, oldest first, with a fresh attempt budget.

    ``rate`` caps how many are published per second, so replaying thousands of
    failures does not hammer the dependency that caused them.
    """
    if limit is not None and limit < 1:
        raise ConfigurationError("limit must be at least 1")
    if rate is not None and rate <= 0:
        raise ConfigurationError("rate must be positive")
    result = BulkResult(dry_run=dry_run)
    batch_size = max(1, min(REPLAY_BATCH, int(rate))) if rate else REPLAY_BATCH
    batch: list[DeadLetter] = []

    async def flush() -> None:
        started = time.monotonic()
        outcomes = await asyncio.gather(*(_replay_one(app, broker, d) for d in batch))
        for outcome in outcomes:
            if outcome == "requeued":
                result.requeued += 1
            elif outcome == "skipped":
                result.skipped += 1
            else:
                result.unreplayable += 1
        sent = len(batch)
        batch.clear()
        if rate:
            await asyncio.sleep(max(0.0, sent / rate - (time.monotonic() - started)))

    async for dead in scan(app, broker, filt, oldest_first=True):
        result.matched += 1
        result.ids.append(dead.id)
        if not dry_run:
            batch.append(dead)
            if len(batch) >= batch_size:
                await flush()
        if limit is not None and result.matched >= limit:
            break
    if batch:
        await flush()
    return result


async def purge_matching(
    app: Queue, broker: Broker, filt: DeadLetterFilter, *, dry_run: bool = False
) -> int:
    """Delete matching dead letters; returns how many were (or, dry run, would be) deleted."""
    ids = [d.id async for d in scan(app, broker, filt)]
    if dry_run:
        return len(ids)
    deleted = 0
    for start in range(0, len(ids), CHUNK):
        deleted += await broker.delete_dead_letters(ids[start : start + CHUNK])
    return deleted


def _group_key(by: str, dead: DeadLetter, serializer: Any) -> str:
    if by == "task":
        return dead.task or "-"
    if by == "queue":
        return dead.queue
    if by == "reason":
        return dead.reason
    if by == "error_type":
        return dead.error.type if dead.error else "-"
    env = _envelope(dead, serializer)
    if by == "rate_key":
        return (env.rate_key if env else None) or "-"
    return ((env.headers or {}).get(by[len("header:") :]) if env else None) or "-"


async def summarize(
    app: Queue, broker: Broker, filt: DeadLetterFilter, *, by: str = "error_type"
) -> dict[str, int]:
    """Count matching dead letters grouped by ``by``, largest group first.

    ``by`` is one of ``task``, ``queue``, ``reason``, ``error_type``, ``rate_key``
    or ``header:<name>`` (for example ``header:country``).
    """
    if by not in _FIELD_GROUPS and not (by.startswith("header:") and len(by) > len("header:")):
        raise ConfigurationError(
            f"cannot group dead letters by {by!r}; use one of {', '.join(_FIELD_GROUPS)} "
            "or header:<name>"
        )
    counts: Counter[str] = Counter()
    async for dead in scan(app, broker, filt):
        counts[_group_key(by, dead, app.serializer)] += 1
    return dict(counts.most_common())
