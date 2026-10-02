"""Idempotency keys.

A key names one logical job. Two layers use it, both through the broker:

* **Enqueue.** ``publish_deduplicated`` claims the key atomically before
  publishing. A second enqueue with the same key (a retried HTTP request, a
  double click, a producer that crashed and restarted) publishes nothing and
  gets a handle to the task that already holds the key.
* **Execution.** The worker takes a leased lock for the key before running the
  task body and records the result when it succeeds. A duplicate message, a
  redelivery after the lease was lost, or a second worker racing the first
  either waits (still running) or returns the recorded result (already done)
  instead of running the body again. A crashed owner's lock expires by itself.

What this cannot cover: a crash after the body's side effect but before the
result is recorded. That window is closed only by passing the key to the
system holding the side effect (``current_task().idempotency_key``).
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING

from .exceptions import ConfigurationError
from .results import TaskHandle

if TYPE_CHECKING:
    from collections.abc import Sequence
    from typing import Any

    from .broker.base import Broker, PublishRequest

MAX_KEY_LENGTH = 512


def validate_key(value: object, task_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ConfigurationError(
            f"idempotency key for task {task_name!r} must be a non-empty string, got {value!r}"
        )
    if len(value) > MAX_KEY_LENGTH:
        raise ConfigurationError(
            f"idempotency key for task {task_name!r} is {len(value)} characters; "
            f"the limit is {MAX_KEY_LENGTH}"
        )
    return value


def storage_key(task_name: str, key: str) -> str:
    """Scope a key to its task, so two tasks can use the same business id."""
    return f"{task_name}|{key}"


async def publish_deduplicated(
    broker: Broker, built: Sequence[tuple[PublishRequest, TaskHandle[Any]]]
) -> list[TaskHandle[Any]]:
    """Publish ``built`` requests, skipping those whose idempotency key is taken.

    Returns one handle per request, in order. A skipped request's handle points
    at the task that already holds the key.
    """
    reqs = [r for r, _ in built]
    handles = [h for _, h in built]
    claimed = [i for i, r in enumerate(reqs) if r.idem_key is not None]
    if not claimed:
        if reqs:
            await broker.publish(reqs)
        return handles

    outcomes = await asyncio.gather(
        *(
            broker.idem_claim(reqs[i].idem_key or "", reqs[i].task_id, reqs[i].idem_ttl)
            for i in claimed
        ),
        return_exceptions=True,
    )
    owned: list[int] = []
    skipped: set[int] = set()
    failure: BaseException | None = None
    for i, outcome in zip(claimed, outcomes, strict=True):
        if isinstance(outcome, BaseException):
            failure = failure or outcome
        elif outcome is None or outcome == reqs[i].task_id:
            owned.append(i)
        else:
            skipped.add(i)
            handles[i] = TaskHandle(outcome, handles[i].app, handles[i].queue)

    try:
        if failure is not None:
            raise failure
        to_publish = [r for i, r in enumerate(reqs) if i not in skipped]
        if to_publish:
            await broker.publish(to_publish)
    except BaseException:
        for i in owned:
            with contextlib.suppress(Exception):
                await broker.idem_unclaim(reqs[i].idem_key or "", reqs[i].task_id)
        raise
    return handles
