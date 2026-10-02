"""RateLimit parsing and the token-bucket contract shared by every broker."""

import asyncio
import itertools

import msgspec
import pytest

from blitzq import ConfigurationError, Queue, RateLimit, current_task
from blitzq.broker import MemoryBroker
from blitzq.ratelimit import as_rate_limit
from blitzq.serialization import Envelope
from conftest import running, wait_for


@pytest.mark.parametrize(
    ("spec", "count", "period"),
    [
        ("10/s", 10, 1.0),
        ("10", 10, 1.0),
        ("100/m", 100, 60.0),
        ("100/min", 100, 60.0),
        ("2.5/second", 2.5, 1.0),
        ("1000/h", 1000, 3600.0),
        ("1/hour", 1, 3600.0),
    ],
)
def test_parse(spec, count, period):
    rl = RateLimit.parse(spec)
    assert rl.count == count
    assert rl.period_seconds == period
    assert rl.rate == count / period


@pytest.mark.parametrize("bad", ["", "abc", "10/day", "10/", "-5/s", "0/s"])
def test_parse_rejects_invalid(bad):
    with pytest.raises(ConfigurationError):
        RateLimit.parse(bad)


def test_as_rate_limit_passthrough():
    rl = RateLimit(10, 1.0)
    assert as_rate_limit(rl) is rl
    assert as_rate_limit(None) is None
    assert as_rate_limit("10/s") == RateLimit(10.0, 1.0)


def test_rate_limit_rejects_non_positive():
    with pytest.raises(ConfigurationError):
        RateLimit(0, 1.0)
    with pytest.raises(ConfigurationError):
        RateLimit(10, 0)


async def test_memory_broker_token_bucket_burst_then_throttle():
    b = MemoryBroker()
    now = 1000.0
    results = [await b.check_rate_limit("k", rate=5.0, capacity=5.0, now=now) for _ in range(7)]
    assert results[:5] == [0.0] * 5
    assert all(w > 0 for w in results[5:])
    # After enough time passes, a token is available again.
    later = await b.check_rate_limit("k", rate=5.0, capacity=5.0, now=now + 1.0)
    assert later == 0.0


async def test_memory_broker_buckets_are_independent_per_key():
    b = MemoryBroker()
    now = 1000.0
    for _ in range(3):
        assert await b.check_rate_limit("a", rate=3.0, capacity=3.0, now=now) == 0.0
    # "a" is now exhausted, but "b" is untouched.
    assert await b.check_rate_limit("b", rate=3.0, capacity=3.0, now=now) == 0.0
    assert await b.check_rate_limit("a", rate=3.0, capacity=3.0, now=now) > 0.0


async def test_task_rate_limited_bursts_then_throttles(memory_app: Queue):
    times: list[float] = []

    @memory_app.task(rate_limit="5/s")
    async def job(i):
        times.append(asyncio.get_running_loop().time())

    await job.enqueue_many((i,) for i in range(8))
    async with running(memory_app, concurrency=10, schedule_poll_interval=0.02):
        await wait_for(lambda: len(times) == 8, timeout=5)
    gaps = [b - a for a, b in itertools.pairwise(times)]
    # First 5 (the burst) land close together; the rest are spaced out.
    assert sum(1 for g in gaps[:4] if g < 0.05) == 4
    assert any(g > 0.1 for g in gaps[4:])


async def test_rate_limited_task_is_not_counted_as_a_retry(memory_app: Queue):
    attempts: list[int] = []

    @memory_app.task(rate_limit="1/s", retries=0)  # zero retries budget - would fail if consumed
    async def job():
        ctx = current_task()
        assert ctx is not None
        attempts.append(ctx.attempt)

    await job.enqueue_many(() for _ in range(3))
    async with running(memory_app, concurrency=10, schedule_poll_interval=0.02):
        await wait_for(lambda: len(attempts) == 3, timeout=5)
    assert attempts == [1, 1, 1]


async def test_unrelated_task_names_have_independent_limits(memory_app: Queue):
    fast_done = []
    slow_done = []

    @memory_app.task(rate_limit="1/s")
    async def slow():
        slow_done.append(1)

    @memory_app.task
    async def fast():
        fast_done.append(1)

    await slow.enqueue_many(() for _ in range(3))
    await fast.enqueue_many(() for _ in range(50))
    async with running(memory_app, concurrency=10, schedule_poll_interval=0.02):
        await wait_for(lambda: len(fast_done) == 50, timeout=5)
        assert len(slow_done) <= 1  # the unrelated fast task is not throttled by slow's limit


def test_invalid_rate_limit_rejected_at_registration():
    app = Queue(broker=MemoryBroker())
    with pytest.raises(ConfigurationError):
        app.task(rate_limit="not-a-rate")(lambda: None)


async def test_rate_key_budgets_are_independent(memory_app: Queue):
    memory_app.rate_limits["slow"] = RateLimit.parse("1/s")
    memory_app.rate_limits["free"] = RateLimit.parse("1000/s")
    done: list[str] = []

    @memory_app.task
    async def charge(tag):
        done.append(tag)

    await charge.options(rate_key="slow").enqueue_many((("slow",)) for _ in range(5))
    await charge.options(rate_key="free").enqueue_many((("free",)) for _ in range(30))
    async with running(memory_app, concurrency=10, schedule_poll_interval=0.02):
        await wait_for(lambda: done.count("free") == 30, timeout=5)
        # The throttled key's backlog did not hold back the unthrottled one.
        assert done.count("slow") <= 2


async def test_rate_key_bucket_is_shared_across_tasks(memory_app: Queue):
    memory_app.rate_limits["gateway"] = RateLimit.parse("1/s")
    done: list[str] = []

    @memory_app.task
    async def charge():
        done.append("charge")

    @memory_app.task
    async def refund():
        done.append("refund")

    await charge.options(rate_key="gateway").enqueue_many(() for _ in range(3))
    await refund.options(rate_key="gateway").enqueue_many(() for _ in range(3))
    async with running(memory_app, concurrency=10, schedule_poll_interval=0.02):
        await asyncio.sleep(0.3)
        assert len(done) <= 1  # one shared token, not one per task


async def test_rate_key_callable_derives_key_from_arguments(memory_app: Queue):
    memory_app.rate_limits["IN"] = RateLimit.parse("1/s")
    done: list[str] = []

    @memory_app.task(rate_key=lambda country, **_: country)
    async def bill(country):
        done.append(country)

    await bill.enqueue_many(("IN",) for _ in range(4))
    await bill.enqueue_many(("US",) for _ in range(4))
    async with running(memory_app, concurrency=10, schedule_poll_interval=0.02):
        await wait_for(lambda: done.count("US") == 4, timeout=5)
        assert done.count("IN") <= 2  # only IN has a budget; US has none


async def test_rate_key_falls_back_to_task_rate_limit_per_key(memory_app: Queue):
    done: list[str] = []

    @memory_app.task(rate_limit="1/s")
    async def job(tag):
        done.append(tag)

    await job.options(rate_key="IN").enqueue("IN")
    await job.options(rate_key="US").enqueue("US")
    async with running(memory_app, concurrency=10, schedule_poll_interval=0.02):
        # Each key gets its own 1/s bucket, so neither waits on the other.
        await wait_for(lambda: len(done) == 2, timeout=1)


def test_envelope_without_rate_key_still_decodes():
    old_wire = ["id1", "t", "q", [], {}, 1, 1.0, 1.0, None, None, None]
    env = msgspec.convert(old_wire, type=Envelope)
    assert env.rate_key is None


def test_invalid_queue_rate_limit_rejected():
    with pytest.raises(ConfigurationError):
        Queue(broker=MemoryBroker(), rate_limits={"x": "not-a-rate"})
