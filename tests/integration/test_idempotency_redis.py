"""Idempotency keys against a real Redis, in both fast and reliable mode."""

import asyncio

import pytest

from blitzq import Queue, RetryPolicy, TaskFailed
from blitzq.task import CallOptions
from conftest import running, wait_for

pytestmark = pytest.mark.redis

FAST_RETRY = RetryPolicy(initial_delay=0.05, max_delay=0.05, backoff=1, jitter=False)


def _unclaimed(task, *args, key):
    req, handle = task._build(args, {}, CallOptions(idempotency_key=key))
    req.idem_key = None
    return req, handle


async def test_duplicate_enqueue_runs_once(redis_app: Queue):
    calls: list[str] = []

    @redis_app.task
    async def charge(order_id):
        calls.append(order_id)
        return {"charged": order_id}

    first = await charge.options(idempotency_key="order-1").enqueue("order-1")
    second = await charge.options(idempotency_key="order-1").enqueue("order-1")
    assert second.id == first.id
    async with running(redis_app):
        assert await second.result(10) == {"charged": "order-1"}
        await asyncio.sleep(0.2)
    assert calls == ["order-1"]


async def test_two_workers_racing_one_key_run_the_body_once(redis_app: Queue):
    release = asyncio.Event()
    started: list[int] = []

    @redis_app.task
    async def slow(n):
        started.append(n)
        await release.wait()
        return {"n": n}

    pairs = [_unclaimed(slow, n, key="k") for n in range(6)]
    async with (
        running(redis_app, concurrency=10, schedule_poll_interval=0.02),
        running(redis_app, concurrency=10, schedule_poll_interval=0.02),
    ):
        await redis_app.broker.publish([r for r, _ in pairs])
        await wait_for(lambda: len(started) >= 1)
        await asyncio.sleep(0.5)
        assert len(started) == 1  # every other delivery is held back
        release.set()
        results = [await h.result(15) for _, h in pairs]
    assert len(started) == 1
    assert all(r == results[0] for r in results)


async def test_redelivery_after_success_replays_the_result(redis_app: Queue):
    calls: list[int] = []

    @redis_app.task
    async def effect(n):
        calls.append(n)
        return {"n": n}

    (req1, h1), (req2, h2) = _unclaimed(effect, 1, key="k"), _unclaimed(effect, 2, key="k")
    async with running(redis_app, concurrency=1):
        await redis_app.broker.publish([req1])
        assert await h1.result(10) == {"n": 1}
        await redis_app.broker.publish([req2])
        assert await h2.result(10) == {"n": 1}
    assert calls == [1]


async def test_failed_attempt_lets_the_retry_run(redis_app: Queue):
    attempts: list[int] = []

    @redis_app.task(retries=2, retry_policy=FAST_RETRY)
    async def flaky():
        attempts.append(1)
        if len(attempts) == 1:
            raise ValueError("transient")
        return "ok"

    handle = await flaky.options(idempotency_key="k").enqueue()
    async with running(redis_app, schedule_poll_interval=0.02):
        assert await handle.result(10) == "ok"
    assert len(attempts) == 2


async def test_key_can_be_resubmitted_after_a_terminal_failure(redis_app: Queue):
    @redis_app.task(retries=0)
    async def boom():
        raise ValueError("permanent")

    first = await boom.options(idempotency_key="k").enqueue()
    async with running(redis_app):
        with pytest.raises(TaskFailed):
            await first.result(10)

        async def resubmitted() -> bool:
            return (await boom.options(idempotency_key="k").enqueue()).id != first.id

        await wait_for(resubmitted)


async def test_crashed_owners_lock_expires(redis_app: Queue):
    b = redis_app.broker
    assert (await b.idem_begin("k", "dead", 0.2))[0] == "run"
    state, left = await b.idem_begin("k", "other", 5)
    assert state == "busy" and left > 0
    await asyncio.sleep(0.4)
    assert (await b.idem_begin("k", "other", 5))[0] == "run"


async def test_first_recorded_result_wins_and_done_is_never_released(redis_app: Queue):
    b = redis_app.broker
    await b.idem_begin("k", "a", 5)
    await b.idem_finish("k", "a", "t1", True, b"first", 60)
    assert await b.idem_begin("k", "b", 5) == ("done", b"first")
    await b.idem_finish("k", "b", "t2", True, b"second", 60)
    assert await b.idem_begin("k", "c", 5) == ("done", b"first")
    await b.idem_unclaim("k", "t1")
    assert await b.idem_claim("k", "t9", 60) == "t1"


async def test_binary_results_survive_the_round_trip(redis_app: Queue):
    b = redis_app.broker
    payload = bytes(range(256))
    await b.idem_begin("bin", "a", 5)
    await b.idem_finish("bin", "a", "t1", True, payload, 60)
    assert await b.idem_begin("bin", "b", 5) == ("done", payload)
