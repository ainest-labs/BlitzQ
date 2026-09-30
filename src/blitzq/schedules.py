"""Schedules for periodic tasks: fixed intervals and cron expressions.

Occurrence identity
-------------------
Every schedule produces a deterministic sequence of *occurrences* (epoch
timestamps). Interval schedules are aligned to the Unix epoch plus an
optional offset (``every=300`` fires at :00, :05, :10, ... UTC), so every
scheduler instance computes the same occurrences without coordination. The
scheduler records the last dispatched occurrence per periodic task in the
broker with an atomic compare-and-set, which is what prevents duplicate
dispatch by concurrent schedulers.

Cron expressions use five fields (minute hour day-of-month month day-of-week)
with ``*``, lists, ranges and steps, plus ``@hourly``, ``@daily``,
``@weekly``, ``@monthly`` and ``@yearly``. They are evaluated in the given
IANA timezone (default UTC). Local times skipped by a DST transition do not
fire; local times repeated by a DST transition fire once (the first time).
When both day-of-month and day-of-week are restricted, a day matches if
either matches (traditional cron semantics).
"""

from __future__ import annotations

import math
from abc import ABC, abstractmethod
from datetime import UTC, datetime, timedelta, tzinfo
from zoneinfo import ZoneInfo


class Schedule(ABC):
    @abstractmethod
    def next_after(self, ts: float) -> float:
        """First occurrence strictly after epoch timestamp ``ts``."""

    def occurrences(self, after: float, until: float, limit: int = 10_000) -> list[float]:
        """Occurrences in ``(after, until]``, oldest first, at most ``limit`` (the latest kept)."""
        out: list[float] = []
        t = self.next_after(after)
        while t <= until:
            out.append(t)
            if len(out) > limit:
                del out[0]
            t = self.next_after(t)
        return out


class Every(Schedule):
    """Fixed interval in seconds, aligned to the epoch plus ``offset``."""

    def __init__(self, seconds: float | timedelta, offset: float = 0.0) -> None:
        if isinstance(seconds, timedelta):
            seconds = seconds.total_seconds()
        if seconds <= 0:
            raise ValueError("interval must be positive")
        self.interval = float(seconds)
        self.offset = float(offset) % self.interval

    def next_after(self, ts: float) -> float:
        k = math.floor((ts - self.offset) / self.interval) + 1
        return k * self.interval + self.offset

    def occurrences(self, after: float, until: float, limit: int = 10_000) -> list[float]:
        # Closed form: avoids iterating over long outages.
        first = self.next_after(after)
        if first > until:
            return []
        n = math.floor((until - first) / self.interval) + 1
        start = max(0, n - limit)
        return [first + i * self.interval for i in range(start, n)]

    def __repr__(self) -> str:
        return f"Every({self.interval:g}s)"


_ALIASES = {
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",
    "@weekly": "0 0 * * 0",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly": "0 * * * *",
}


def _parse_field(spec: str, lo: int, hi: int) -> frozenset[int]:
    values: set[int] = set()
    for part in spec.split(","):
        step = 1
        if "/" in part:
            part, step_s = part.split("/", 1)
            step = int(step_s)
            if step <= 0:
                raise ValueError(f"invalid step in {spec!r}")
        if part == "*":
            start, end = lo, hi
        elif "-" in part:
            a, b = part.split("-", 1)
            start, end = int(a), int(b)
        else:
            start = int(part)
            end = hi if step != 1 else start
        if start < lo or end > hi or start > end:
            raise ValueError(f"value out of range in {spec!r} (allowed {lo}-{hi})")
        values.update(range(start, end + 1, step))
    return frozenset(values)


class Cron(Schedule):
    """Five-field cron expression evaluated in ``tz`` (IANA name or tzinfo)."""

    def __init__(self, expr: str, tz: str | tzinfo | None = None) -> None:
        self.expr = expr
        fields = _ALIASES.get(expr.strip(), expr).split()
        if len(fields) != 5:
            raise ValueError(f"cron expression must have 5 fields: {expr!r}")
        try:
            self.minutes = _parse_field(fields[0], 0, 59)
            self.hours = _parse_field(fields[1], 0, 23)
            self.days = _parse_field(fields[2], 1, 31)
            self.months = _parse_field(fields[3], 1, 12)
            dow = _parse_field(fields[4], 0, 7)
        except ValueError as exc:
            raise ValueError(f"invalid cron expression {expr!r}: {exc}") from None
        self.weekdays = frozenset(d % 7 for d in dow)  # 0 and 7 are Sunday
        self.dom_restricted = fields[2] != "*"
        self.dow_restricted = fields[4] != "*"
        self.tz: tzinfo = ZoneInfo(tz) if isinstance(tz, str) else (tz or UTC)
        self._sorted_minutes = sorted(self.minutes)

    def _day_matches(self, d: datetime) -> bool:
        dom = d.day in self.days
        dow = (d.isoweekday() % 7) in self.weekdays  # cron: 0 = Sunday
        if self.dom_restricted and self.dow_restricted:
            return dom or dow
        return dom and dow

    def _next_local(self, t: datetime) -> datetime:
        """Next matching naive local wall time >= ``t`` (minute resolution)."""
        limit = t.year + 5
        while t.year <= limit:
            if t.month not in self.months:
                t = (t.replace(day=1, hour=0, minute=0) + timedelta(days=32)).replace(day=1)
                continue
            if not self._day_matches(t):
                t = (t + timedelta(days=1)).replace(hour=0, minute=0)
                continue
            if t.hour not in self.hours:
                t = (t + timedelta(hours=1)).replace(minute=0)
                continue
            nxt = next((m for m in self._sorted_minutes if m >= t.minute), None)
            if nxt is None:
                t = (t + timedelta(hours=1)).replace(minute=0)
                continue
            return t.replace(minute=nxt)
        raise ValueError(f"cron expression {self.expr!r} has no occurrence within 5 years")

    def next_after(self, ts: float) -> float:
        local = datetime.fromtimestamp(ts, self.tz).replace(tzinfo=None, second=0, microsecond=0)
        t = local + timedelta(minutes=1)
        while True:
            t = self._next_local(t)
            aware = t.replace(tzinfo=self.tz, fold=0)
            epoch = aware.timestamp()
            # Skip wall times that do not exist in this zone (DST gap).
            roundtrip = datetime.fromtimestamp(epoch, self.tz).replace(tzinfo=None)
            if roundtrip == t and epoch > ts:
                return epoch
            t += timedelta(minutes=1)

    def __repr__(self) -> str:
        return f"Cron({self.expr!r}, tz={self.tz})"


def as_schedule(value: Schedule | str | float | timedelta, tz: str | None = None) -> Schedule:
    if isinstance(value, Schedule):
        return value
    if isinstance(value, str):
        return Cron(value, tz)
    return Every(value)
