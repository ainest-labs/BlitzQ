"""Task rate limiting.

A rate limit caps how often a task *starts* execution, enforced across every
worker sharing the broker (a Redis-backed token bucket, not a per-process
counter), so ``rate_limit="10/s"`` means 10/s total no matter how many
workers or processes are running it.

A task over its limit is not executed and not counted as a retry: the worker
puts the message back into the schedule (the same sorted set delayed tasks
use) for the wait time the bucket reports, then tries again - the same
"never sleep, reschedule instead" principle retries already follow. This
means backpressure comes from the queue depth growing, not from a worker
thread blocking.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from .exceptions import ConfigurationError

_PATTERN = re.compile(r"^(\d+(?:\.\d+)?)\s*(?:/\s*(s|sec|second|m|min|minute|h|hour))?$")
_PERIOD_SECONDS = {
    None: 1.0,
    "s": 1.0,
    "sec": 1.0,
    "second": 1.0,
    "m": 60.0,
    "min": 60.0,
    "minute": 60.0,
    "h": 3600.0,
    "hour": 3600.0,
}


@dataclass(frozen=True, slots=True)
class RateLimit:
    """A cap of ``count`` task starts per ``period_seconds``, enforced globally.

    The bucket holds up to ``count`` tokens (one burst's worth) and refills
    continuously at ``count / period_seconds`` tokens/second, so a limit of
    ``"100/m"`` allows a burst of 100 immediately after being idle, then
    settles to roughly one every 0.6s - it does not release exactly 100 once
    a minute.
    """

    count: float
    period_seconds: float

    def __post_init__(self) -> None:
        if self.count <= 0 or self.period_seconds <= 0:
            raise ConfigurationError("rate limit count and period must be positive")

    @property
    def rate(self) -> float:
        """Tokens per second."""
        return self.count / self.period_seconds

    @classmethod
    def parse(cls, spec: str) -> RateLimit:
        """Parse ``"100"``, ``"100/s"``, ``"100/m"`` or ``"100/hour"``."""
        m = _PATTERN.match(spec.strip())
        if not m:
            raise ConfigurationError(
                f"invalid rate limit {spec!r}; expected e.g. '10/s', '100/m', '1000/hour'"
            )
        count, unit = m.groups()
        return cls(float(count), _PERIOD_SECONDS[unit])


def as_rate_limit(value: RateLimit | str | None) -> RateLimit | None:
    if value is None or isinstance(value, RateLimit):
        return value
    return RateLimit.parse(value)
