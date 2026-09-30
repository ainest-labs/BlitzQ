import random
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from blitzq import Cron, Every, Retry, RetryPolicy
from blitzq.routing import Router


# -- retry policy ----------------------------------------------------------------------
def test_exponential_backoff_without_jitter():
    p = RetryPolicy(initial_delay=1, backoff=2, max_delay=10, jitter=False)
    assert [p.compute_delay(n) for n in range(1, 7)] == [1, 2, 4, 8, 10, 10]


def test_jitter_stays_within_half_to_full_delay():
    p = RetryPolicy(initial_delay=4, backoff=1, jitter=True)
    rng = random.Random(1)
    delays = [p.compute_delay(1, rng) for _ in range(500)]
    assert all(2 <= d <= 4 for d in delays)
    assert max(delays) - min(delays) > 1  # actually randomised


def test_huge_retry_numbers_do_not_overflow():
    p = RetryPolicy(initial_delay=1, backoff=10, max_delay=60, jitter=False)
    assert p.compute_delay(10_000) == 60


def test_exception_classification():
    p = RetryPolicy(retry_on=(ConnectionError, TimeoutError), dont_retry_on=(ConnectionResetError,))
    assert p.is_retryable(ConnectionError())
    assert p.is_retryable(TimeoutError())
    assert not p.is_retryable(ConnectionResetError())  # dont_retry_on wins
    assert not p.is_retryable(ValueError())
    assert p.is_retryable(Retry())  # explicit retry always honoured


@pytest.mark.parametrize("kw", [{"initial_delay": -1}, {"backoff": 0.5}])
def test_invalid_policy(kw):
    with pytest.raises(ValueError):
        RetryPolicy(**kw)


# -- routing ---------------------------------------------------------------------------
def test_routing_precedence():
    r = Router({"app.images.*": "images", "app.*": "misc"}, default="default")
    assert r.resolve("app.images.resize", None, None) == "images"
    assert r.resolve("app.other", None, None) == "misc"
    assert r.resolve("lib.x", None, None) == "default"
    assert r.resolve("app.images.resize", "decorated", None) == "decorated"
    assert r.resolve("app.images.resize", "decorated", "call") == "call"


def test_routing_callable():
    r = Router(lambda name: "emails" if "mail" in name else None, default="default")
    assert r.resolve("send_mail", None, None) == "emails"
    assert r.resolve("other", None, None) == "default"


# -- schedules -------------------------------------------------------------------------
def ts(*args, tz=UTC):
    return datetime(*args, tzinfo=tz).timestamp()


def test_every_is_epoch_aligned_and_deterministic():
    e = Every(300)
    assert e.next_after(ts(2026, 1, 1, 0, 2)) == ts(2026, 1, 1, 0, 5)
    assert e.next_after(ts(2026, 1, 1, 0, 5)) == ts(2026, 1, 1, 0, 10)
    assert Every(timedelta(minutes=5)).next_after(123.0) == e.next_after(123.0)


def test_every_occurrences_closed_form_with_limit():
    e = Every(10)
    occ = e.occurrences(0, 100)
    assert occ == [10.0 * i for i in range(1, 11)]
    assert e.occurrences(0, 1_000_000, limit=3) == [999_980.0, 999_990.0, 1_000_000.0]
    assert e.occurrences(5, 9) == []


@pytest.mark.parametrize(
    ("expr", "start", "expected"),
    [
        ("*/15 * * * *", (2026, 3, 1, 10, 7), (2026, 3, 1, 10, 15)),
        ("0 9 * * 1-5", (2026, 3, 6, 9, 0), (2026, 3, 9, 9, 0)),  # Fri 9:00 -> Mon 9:00
        ("30 2 1 * *", (2026, 1, 15, 0, 0), (2026, 2, 1, 2, 30)),
        ("0 0 29 2 *", (2026, 1, 1, 0, 0), (2028, 2, 29, 0, 0)),  # leap day
        ("@hourly", (2026, 1, 1, 5, 59), (2026, 1, 1, 6, 0)),
        ("0 12 13 * 5", (2026, 3, 1, 0, 0), (2026, 3, 6, 12, 0)),  # dom OR dow
        ("0 0 * * 7", (2026, 3, 2, 0, 0), (2026, 3, 8, 0, 0)),  # 7 == Sunday
    ],
)
def test_cron_next(expr, start, expected):
    assert Cron(expr).next_after(ts(*start)) == ts(*expected)


def test_cron_is_strictly_after():
    c = Cron("0 * * * *")
    t = ts(2026, 1, 1, 5, 0)
    assert c.next_after(t) == ts(2026, 1, 1, 6, 0)


def test_cron_dst_spring_forward_skips_missing_time():
    tz = ZoneInfo("America/New_York")
    c = Cron("30 2 * * *", tz="America/New_York")
    # 2026-03-08 02:30 does not exist in New York; next run is 03-09 02:30.
    start = datetime(2026, 3, 7, 12, 0, tzinfo=tz).timestamp()
    first = c.next_after(start)
    assert datetime.fromtimestamp(first, tz) == datetime(2026, 3, 9, 2, 30, tzinfo=tz)


def test_cron_dst_fall_back_runs_once():
    tz = ZoneInfo("America/New_York")
    c = Cron("30 1 * * *", tz="America/New_York")
    start = datetime(2026, 10, 31, 12, 0, tzinfo=tz).timestamp()
    occ = c.occurrences(start, start + 2 * 86400)
    local = [datetime.fromtimestamp(o, tz) for o in occ]
    assert [(d.day, d.hour, d.minute) for d in local] == [(1, 1, 30), (2, 1, 30)]


def test_cron_timezone():
    c = Cron("0 9 * * *", tz="Asia/Kolkata")
    nxt = c.next_after(ts(2026, 1, 1, 0, 0))
    assert nxt == ts(2026, 1, 1, 3, 30)  # 09:00 IST == 03:30 UTC


@pytest.mark.parametrize(
    "bad", ["* * * *", "60 * * * *", "* 24 * * *", "*/0 * * * *", "5-1 * * * *"]
)
def test_invalid_cron(bad):
    with pytest.raises(ValueError):
        Cron(bad)
