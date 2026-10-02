"""Idempotency keys: enqueue-time dedup and execution-time exactly-once effects."""

import asyncio

import pytest

from blitzq import ConfigurationError, Queue, RetryPolicy, TaskFailed, Worker, current_task
from blitzq.broker import MemoryBroker
from blitzq.task import CallOptions
from conftest import running, wait_for

FAST_RETRY = RetryPolicy(initial_delay=0.05, max_delay=0.05, backoff=1, jitter=False)


def _unclaimed(task, *args, key):
    """A published message that bypassed the enqueue-time claim.

    Models what the worker still has to survive: a claim that expired, a
    duplicate that slipped past, or a message redelivered after its lease was lost.
    """
    req, handle = task._build(args, {}, CallOptions(idempotency_key=key))
    req.idem_key = None
    return req, handle


async def test_duplicate_enqueue_runs_once_and_shares_the_result(memory_app: Queue):
    calls: list[str] = []

    @memory_app.task
    async def charge(order_id):
        calls.append(order_id)
        return {"charged": order_id}

    first = await charge.options(idempotency_key="order-1").enqueue("order-1")
    second = await charge.options(idempotency_key="order-1").enqueue("order-1")
    assert second.id == first.id
    async with running(memory_app):
        assert await second.result(5) == {"charged": "order-1"}
    assert calls == ["order-1"]


async def test_duplicates_inside_one_batch_publish_once(memory_app: Queue):
    calls: list[str] = []

    @memory_app.task
    async def charge(tag):
        calls.append(tag)

    handles = await charge.options(idempotency_key="k").enqueue_many([("a",), ("b",), ("c",)])
    assert len({h.id for h in handles}) == 1
    async with running(memory_app):
        await wait_for(lambda: calls)
        await asyncio.sleep(0.1)
    assert len(calls) == 1


async def test_distinct_keys_all_run(memory_app: Queue):
    calls: list[int] = []

    @memory_app.task
    async def job(n):
        calls.append(n)

    for n in range(5):
        await job.options(idempotency_key=f"k{n}").enqueue(n)
    async with running(memory_app):
        await wait_for(lambda: len(calls) == 5)


async def test_key_is_scoped_to_the_task(memory_app: Queue):
    ran: list[str] = []

    @memory_app.task
    async def charge():
        ran.append("charge")

    @memory_app.task
    async def notify():
        ran.append("notify")

    a = await charge.options(idempotency_key="order-1").enqueue()
    b = await notify.options(idempotency_key="order-1").enqueue()
    assert a.id != b.id
    async with running(memory_app):
        await wait_for(lambda: sorted(ran) == ["charge", "notify"])


async def test_callable_key_is_derived_from_the_arguments(memory_app: Queue):
    calls: list[int] = []

    @memory_app.task(idempotency_key=lambda order_id, **_: f"order:{order_id}")
    async def bill(order_id):
        calls.append(order_id)

    h1 = await bill.enqueue(7)
    h2 = await bill.enqueue(7)
    h3 = await bill.enqueue(8)
    assert h1.id == h2.id and h3.id != h1.id
    async with running(memory_app):
        await wait_for(lambda: sorted(calls) == [7, 8])


async def test_task_sees_its_key(memory_app: Queue):
    seen: list[str | None] = []

    @memory_app.task
    async def job():
        ctx = current_task()
        assert ctx is not None
        seen.append(ctx.idempotency_key)

    @memory_app.task
    async def plain():
        ctx = current_task()
        assert ctx is not None
        seen.append(ctx.idempotency_key)

    await job.options(idempotency_key="pay-42").enqueue()
    await plain.enqueue()
    async with running(memory_app):
        await wait_for(lambda: len(seen) == 2)
    assert sorted(seen, key=str) == [None, "pay-42"]


async def test_redelivered_message_returns_the_recorded_result_without_rerunning(
    memory_app: Queue,
):
    calls: list[int] = []

    @memory_app.task
    async def effect(n):
        calls.append(n)
        return {"n": n}

    (req1, h1), (req2, h2) = _unclaimed(effect, 1, key="k"), _unclaimed(effect, 2, key="k")
    async with running(memory_app, concurrency=1):
        await memory_app.broker.publish([req1])
        assert await h1.result(5) == {"n": 1}
        await memory_app.broker.publish([req2])
        # A different task id, same key: replayed result of the first, body skipped.
        assert await h2.result(5) == {"n": 1}
    assert calls == [1]


async def test_duplicate_waits_for_a_running_execution(memory_app: Queue):
    release = asyncio.Event()
    started: list[int] = []

    @memory_app.task
    async def slow(n):
        started.append(n)
        await release.wait()
        return {"n": n}

    (req1, h1), (req2, h2) = _unclaimed(slow, 1, key="k"), _unclaimed(slow, 2, key="k")
    async with running(memory_app, concurrency=4, schedule_poll_interval=0.02):
        await memory_app.broker.publish([req1, req2])
        await wait_for(lambda: len(started) == 1)
        await asyncio.sleep(0.3)
        assert len(started) == 1  # the duplicate is held back, not run concurrently
        release.set()
        r1, r2 = await h1.result(5), await h2.result(5)
    assert len(started) == 1
    assert r1 == r2


async def test_failed_attempt_lets_the_retry_run(memory_app: Queue):
    attempts: list[int] = []

    @memory_app.task(retries=2, retry_policy=FAST_RETRY)
    async def flaky():
        attempts.append(1)
        if len(attempts) == 1:
            raise ValueError("transient")
        return "ok"

    handle = await flaky.options(idempotency_key="k").enqueue()
    async with running(memory_app, schedule_poll_interval=0.02):
        assert await handle.result(5) == "ok"
    assert len(attempts) == 2


async def test_key_can_be_resubmitted_after_a_terminal_failure(memory_app: Queue):
    runs: list[int] = []

    @memory_app.task(retries=0)
    async def boom():
        runs.append(1)
        raise ValueError("permanent")

    first = await boom.options(idempotency_key="k").enqueue()
    async with running(memory_app):
        with pytest.raises(TaskFailed):
            await first.result(5)
        await wait_for(lambda: not memory_app.broker._idem)
        second = await boom.options(idempotency_key="k").enqueue()
        assert second.id != first.id
        with pytest.raises(TaskFailed):
            await second.result(5)
    assert len(runs) == 2


async def test_completed_key_is_never_released(memory_app: Queue):
    @memory_app.task
    async def job():
        return 1

    first = await job.options(idempotency_key="k").enqueue()
    async with running(memory_app):
        assert await first.result(5) == 1
    await memory_app.broker.idem_unclaim("job|k", first.id)
    again = await job.options(idempotency_key="k").enqueue()
    assert again.id == first.id


def test_enqueue_sync_deduplicates(memory_app: Queue):
    @memory_app.task
    def job(x):
        return x

    first = job.options(idempotency_key="s").enqueue_sync("x")
    second = job.options(idempotency_key="s").enqueue_sync("x")
    assert second.id == first.id


async def test_send_by_name_accepts_a_key(memory_app: Queue):
    @memory_app.task(name="remote.job")
    async def job():
        return 1

    first = await memory_app.send("remote.job", idempotency_key="n")
    second = await memory_app.send("remote.job", idempotency_key="n")
    assert second.id == first.id


@pytest.mark.parametrize("bad", ["", "x" * 513])
async def test_invalid_key_is_rejected(memory_app: Queue, bad):
    @memory_app.task
    async def job(): ...

    with pytest.raises(ConfigurationError):
        await job.options(idempotency_key=bad).enqueue()


async def test_callable_key_must_return_a_string(memory_app: Queue):
    @memory_app.task(idempotency_key=lambda n: n)
    async def job(n): ...

    with pytest.raises(ConfigurationError):
        await job.enqueue(5)


def test_idempotency_ttl_must_be_positive():
    with pytest.raises(ConfigurationError):
        Queue(broker=MemoryBroker(), idempotency_ttl=0)


async def test_publish_failure_releases_the_claim(memory_app: Queue):
    @memory_app.task
    async def job(): ...

    original = memory_app.broker.publish

    async def broken(requests):
        raise ConnectionError("redis down")

    memory_app.broker.publish = broken  # type: ignore[method-assign]
    with pytest.raises(ConnectionError):
        await job.options(idempotency_key="k").enqueue()
    memory_app.broker.publish = original  # type: ignore[method-assign]
    retry = await job.options(idempotency_key="k").enqueue()
    assert retry.id  # not blocked by the failed attempt's claim


# -- broker contract (shared by every broker) ---------------------------------------


async def test_broker_claim_returns_the_holder():
    b = MemoryBroker()
    assert await b.idem_claim("k", "t1", 60) is None
    assert await b.idem_claim("k", "t2", 60) == "t1"
    await b.idem_unclaim("k", "t2")  # not the holder: no effect
    assert await b.idem_claim("k", "t3", 60) == "t1"
    await b.idem_unclaim("k", "t1")
    assert await b.idem_claim("k", "t3", 60) is None


async def test_broker_lock_is_exclusive_and_renewable():
    b = MemoryBroker()
    assert (await b.idem_begin("k", "a", 5))[0] == "run"
    state, left = await b.idem_begin("k", "b", 5)
    assert state == "busy" and 0 < left <= 5
    assert await b.idem_renew("k", "a", 5) is True
    assert await b.idem_renew("k", "b", 5) is False


async def test_broker_crashed_owners_lock_expires():
    b = MemoryBroker()
    assert (await b.idem_begin("k", "dead", 0.05))[0] == "run"
    assert (await b.idem_begin("k", "other", 5))[0] == "busy"
    await asyncio.sleep(0.1)
    assert (await b.idem_begin("k", "other", 5))[0] == "run"


async def test_broker_first_recorded_result_wins():
    b = MemoryBroker()
    await b.idem_begin("k", "a", 5)
    await b.idem_finish("k", "a", "t1", True, b"first", 60)
    assert await b.idem_begin("k", "b", 5) == ("done", b"first")
    await b.idem_finish("k", "b", "t2", True, b"second", 60)
    assert await b.idem_begin("k", "c", 5) == ("done", b"first")


async def test_broker_failed_attempt_only_releases_the_lock():
    b = MemoryBroker()
    await b.idem_begin("k", "a", 5)
    await b.idem_finish("k", "a", "t1", False, b"", 60)
    assert (await b.idem_begin("k", "b", 5))[0] == "run"


async def test_broker_success_is_recorded_even_if_the_lock_was_lost():
    b = MemoryBroker()
    await b.idem_begin("k", "slow", 0.05)
    await asyncio.sleep(0.1)
    assert (await b.idem_begin("k", "taker", 5))[0] == "run"
    await b.idem_finish("k", "slow", "t1", True, b"r", 60)  # the effect did happen
    assert await b.idem_begin("k", "late", 5) == ("done", b"r")


async def test_only_locks_of_running_tasks_are_renewed(memory_app: Queue):
    """A lock leaked by an internal error must lapse, not be renewed forever."""
    worker = Worker(memory_app)
    renewed: list[str] = []

    async def spy(key, owner, lease):
        renewed.append(owner)
        return True

    memory_app.broker.idem_renew = spy  # type: ignore[method-assign]
    worker._idem_locks["leaked"] = ("t|k1", "task-a")
    worker._idem_locks["live"] = ("t|k2", "task-b")
    worker._running["task-b"] = asyncio.current_task()  # type: ignore[assignment]
    await worker._renew_idem_locks()
    assert renewed == ["live"]
