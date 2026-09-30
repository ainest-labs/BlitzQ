"""Retry policies.

Terminology: an *attempt* is one execution of a task. The first execution is
attempt 1; every later attempt is a *retry*. ``retries=3`` therefore allows up
to four attempts in total.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field

from .exceptions import Retry, TaskTimeout

__all__ = ["Retry", "RetryPolicy", "TaskTimeout"]


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Exponential backoff with optional jitter.

    The delay before retry ``n`` (``n = 1`` for the first retry) is
    ``min(max_delay, initial_delay * backoff ** (n - 1))``. With ``jitter``
    enabled the delay is drawn uniformly from ``[delay / 2, delay]`` ("equal
    jitter"), which spreads retries of simultaneously failing tasks while
    keeping a guaranteed minimum wait.

    ``retry_on`` lists exception types that trigger an automatic retry;
    ``dont_retry_on`` takes precedence and marks exceptions as permanent
    failures. An explicit :class:`~blitzq.Retry` raised by the task is always
    honoured while attempts remain.
    """

    initial_delay: float = 1.0
    max_delay: float = 300.0
    backoff: float = 2.0
    jitter: bool = True
    retry_on: tuple[type[BaseException], ...] = (Exception,)
    dont_retry_on: tuple[type[BaseException], ...] = field(default=())

    def __post_init__(self) -> None:
        if self.initial_delay < 0 or self.max_delay < 0:
            raise ValueError("retry delays must be non-negative")
        if self.backoff < 1:
            raise ValueError("backoff must be >= 1")

    def compute_delay(self, retry_number: int, rng: random.Random | None = None) -> float:
        """Delay in seconds before retry number ``retry_number`` (1-based)."""
        n = max(1, retry_number)
        try:
            delay = self.initial_delay * (self.backoff ** (n - 1))
        except OverflowError:
            delay = self.max_delay
        delay = min(self.max_delay, delay)
        if self.jitter and delay > 0:
            delay = (rng or random).uniform(delay / 2, delay)
        return delay

    def is_retryable(self, exc: BaseException) -> bool:
        if isinstance(exc, Retry):
            return True
        if self.dont_retry_on and isinstance(exc, self.dont_retry_on):
            return False
        return isinstance(exc, self.retry_on)


DEFAULT_RETRY_POLICY = RetryPolicy()
