"""End-to-end behaviour against a real Redis, in both fast and reliable mode."""

import asyncio
import itertools
import os
import time
import uuid

import pytest

from blitzq import Queue, RetryPolicy, TaskFailed, TaskState, Worker
from blitzq.broker.base import PublishRequest
from conftest import running, unique_ns, wait_for

pytestmark = pytest.mark.redis


async def test_enqueue_and_result(redis_app: Queue):
    @redis_app.task
    async def add(a, b):
        return {"sum": a + b}

    @redis_app.task
    def upper(s):
        return s.upper()

    async with running(redis_app):
        h1 = await add.enqueue(1, 2)
        h2 = await upper.enqueue("x")
        assert await redis_app.get_result(h1.id, timeout=10) == {"sum": 3}
        assert await h2.result(10) == "X"
        info = await h1.info()
        assert info is not None and info.state == TaskState.SUCCEEDED
        assert info.attempt == 1 and info.duration is not None


async def test_process_executor(redis_app_factory):
    import cpu_tasks

    app = redis_app_factory("reliable")
    fib = app.task(executor="process")(cpu_tasks.fib)
    async with running(app, processes=2):
        handles = [await fib.enqueue(30) for _ in range(4)]
        results = [await h.result(60) for h in handles]
    assert {r[0] for r in results} == {832040}
    assert all(pid != os.getpid() for _, pid in results)


async def test_multiple_queues_concurrently(redis_app: Queue):
    done: dict[str, int] = {"a": 0, "b": 0, "c": 0}
    for name in done:

        @redis_app.task(queue=name, name=f"t_{name}")
        async def t(name=name):
            done[name] += 1

    async with running(redis_app, queues=["a", "b", "c"], concurrency=20):
        for name in done:
            await redis_app.tasks[f"t_{name}"].enqueue_many(() for _ in range(30))
        await wait_for(lambda: all(v == 30 for v in done.values()), timeout=15)


async def test_busy_queue_does_not_starve_quiet_queue(redis_app: Queue):
    done = {"images": 0, "notify": 0}

    @redis_app.task(queue="images")
    async def resize():
        await asyncio.sleep(0.05)
        done["images"] += 1

    @redis_app.task(queue="notify")
    async def notify():
        done["notify"] += 1

    async with running(
        redis_app, queues=["images", "notify"], concurrency=10, queue_concurrency={"images": 4}
    ):
        await resize.enqueue_many(() for _ in range(200))  # ~2.5s of backlog at 4-wide
        await asyncio.sleep(0.2)
        t0 = time.monotonic()
        await notify.enqueue_many(() for _ in range(20))
        await wait_for(lambda: done["notify"] == 20, timeout=10)
        assert time.monotonic() - t0 < 1.0
        assert done["images"] < 200


async def test_worker_only_consumes_subscribed_queues(redis_app: Queue):
    ran = []

    @redis_app.task(queue="other")
    async def elsewhere():
        ran.append(1)

    @redis_app.task
    async def here():
        return 1

    async with running(redis_app, queues=["default"]):
        await elsewhere.enqueue()
        assert await (await here.enqueue()).result(10) == 1
        await asyncio.sleep(0.2)
    assert ran == []
    assert (await redis_app.queue_stats(["other"]))[0].waiting == 1


async def test_two_workers_share_a_queue_without_duplicates(redis_app: Queue):
    seen: list[int] = []

    @redis_app.task
    async def record(i):
        await asyncio.sleep(0.001)
        seen.append(i)

    async with running(redis_app, concurrency=10), running(redis_app, concurrency=10):
        await record.enqueue_many((i,) for i in range(500))
        await wait_for(lambda: len(seen) >= 500, timeout=20)
        await asyncio.sleep(0.2)
    assert sorted(seen) == list(range(500))


async def test_retries_backoff_and_dead_letter(redis_app: Queue):
    attempts = []
    healthy = False

    @redis_app.task(retries=2, retry_policy=RetryPolicy(initial_delay=0.1, jitter=False))
    async def flaky():
        attempts.append(time.perf_counter())
        if not healthy:
            raise ConnectionError("downstream unavailable")
        return "recovered"

    async with running(redis_app):
        h = await flaky.enqueue()
        with pytest.raises(TaskFailed) as ei:
            await h.result(10)
        assert len(attempts) == 3
        gaps = [b - a for a, b in itertools.pairwise(attempts)]
        # 0.1s then 0.2s backoff. ETAs use wall-clock time, whose resolution is
        # ~16 ms on Windows, so allow one clock tick of tolerance.
        assert gaps[0] >= 0.08 and gaps[1] >= 0.18
        assert gaps[1] > gaps[0] * 1.5
        assert ei.value.state == "dead_lettered"
        dead = await redis_app.dead_letters()
        assert len(dead) == 1 and dead[0].attempt == 3
        assert dead[0].error is not None and dead[0].error.type == "ConnectionError"

        # Replay with a fresh attempt budget once the dependency recovers.
        healthy = True
        assert await redis_app.retry(h.id)
        assert await h.result(10) == "recovered"
    assert await redis_app.broker.dead_letter_count() == 0


async def test_delayed_and_eta(redis_app: Queue):
    from datetime import UTC, datetime, timedelta

    @redis_app.task
    async def stamp():
        return time.time()

    async with running(redis_app):
        t0 = time.time()
        h1 = await stamp.options(delay=0.5).enqueue()
        h2 = await stamp.options(eta=datetime.now(UTC) + timedelta(seconds=0.3)).enqueue()
        assert await redis_app.status(h1.id) == TaskState.SCHEDULED
        scheduled = await redis_app.broker.scheduled()
        assert {s.task_id for s in scheduled} == {h1.id, h2.id}
        r1, r2 = await h1.result(10), await h2.result(10)
    assert r1 - t0 >= 0.49 and r2 - t0 >= 0.29
    assert r2 < r1


async def test_many_delayed_tasks_all_promoted_once(redis_app: Queue):
    seen: list[int] = []

    @redis_app.task
    async def rec(i):
        seen.append(i)

    async with running(redis_app), running(redis_app):  # two concurrent promoters
        for i in range(300):
            await rec.options(delay=0.2 + (i % 10) / 50).enqueue(i)
        await wait_for(lambda: len(seen) >= 300, timeout=15)
        await asyncio.sleep(0.3)
    assert sorted(seen) == list(range(300))


async def test_cancel_scheduled_and_revoke_queued(redis_app: Queue):
    ran = []

    @redis_app.task
    async def job(i):
        ran.append(i)

    h_sched = await job.options(delay=0.3).enqueue(1)
    h_queued = await job.enqueue(2)
    assert await redis_app.cancel(h_sched.id) is True
    assert await redis_app.cancel(h_queued.id) is False
    async with running(redis_app):
        h3 = await job.enqueue(3)
        await h3.result(10)
        for h in (h_sched, h_queued):
            with pytest.raises(TaskFailed) as ei:
                await h.result(10)
            assert ei.value.state == "cancelled"
        await asyncio.sleep(0.4)
    assert ran == [3]


async def test_result_expiration(redis_app_factory, mode):
    app = redis_app_factory(mode, result_ttl=1)

    @app.task
    async def quick():
        return 1

    async with running(app):
        h = await quick.enqueue()
        assert await h.result(10) == 1
    await asyncio.sleep(2.1)
    assert await app.inspect(h.id) is None


async def test_track_state(redis_app_factory, mode):
    app = redis_app_factory(mode, track_state=True)
    gate = asyncio.Event()

    @app.task
    async def job():
        await gate.wait()

    h = await job.enqueue()
    assert await h.status() == TaskState.QUEUED
    async with running(app):
        await wait_for(lambda: _state_is(app, h.id, TaskState.RUNNING))
        gate.set()
        await h.result(10)
    assert await h.status() == TaskState.SUCCEEDED


async def _state_is(app, task_id, state):
    return await app.status(task_id) == state


async def test_malformed_message_is_dead_lettered(redis_app: Queue):
    @redis_app.task
    async def ok():
        return 1

    async with running(redis_app):
        await redis_app.broker.publish([PublishRequest("default", "bad", b"\xc1\xc1")])
        assert await (await ok.enqueue()).result(10) == 1
        await wait_for(redis_app.broker.dead_letter_count, timeout=10)
    dead = await redis_app.dead_letters()
    assert dead[0].reason == "malformed message" and dead[0].message == b"\xc1\xc1"


async def test_sync_api_from_plain_thread(redis_app: Queue):
    @redis_app.task
    def double(x):
        return 2 * x

    def producer():
        h = double.enqueue_sync(21)
        return h, h.result_sync(timeout=10)

    async with running(redis_app):
        handle, value = await asyncio.to_thread(producer)
    assert value == 42
    assert await asyncio.to_thread(redis_app.status_sync, handle.id) == TaskState.SUCCEEDED
    redis_app.close_sync()


async def test_send_by_name(redis_app: Queue):
    @redis_app.task(name="billing.charge")
    async def charge(amount, currency="EUR"):
        return f"{amount} {currency}"

    producer = Queue(
        redis_url=redis_app.redis_url, mode=redis_app.mode, namespace=redis_app.namespace
    )
    try:
        h = await producer.send("billing.charge", args=[5], kwargs={"currency": "USD"})
        async with running(redis_app):
            assert await producer.get_result(h.id, timeout=10) == "5 USD"
    finally:
        await producer.close()


async def test_queue_stats_and_workers(redis_app: Queue):
    @redis_app.task(queue="stats-q")
    async def t():
        pass

    await t.enqueue_many(() for _ in range(5))
    stats = {s.name: s for s in await redis_app.queue_stats(["stats-q"])}
    assert stats["stats-q"].waiting == 5
    async with running(redis_app, queues=["stats-q"], heartbeat_interval=0.1) as w:
        await wait_for(lambda: _waiting(redis_app, "stats-q", 0))
        await wait_for(lambda: _has_worker(redis_app, w.id))


async def _waiting(app, q, n):
    return (await app.queue_stats([q]))[0].waiting == n


async def _has_worker(app, wid):
    return wid in await app.broker.workers()


async def test_namespaces_are_isolated(redis_app_factory, mode):
    a = redis_app_factory(mode)
    b = redis_app_factory(mode)

    @a.task(name="same")
    async def ta():
        return "a"

    @b.task(name="same")
    async def tb():
        return "b"

    async with running(a), running(b):
        assert await (await ta.enqueue()).result(10) == "a"
        assert await (await tb.enqueue()).result(10) == "b"


async def test_explicit_task_id_and_loop_reuse(redis_app_factory):
    """The client works across separate event loops (e.g. repeated asyncio.run)."""
    app = redis_app_factory("reliable")

    @app.task
    async def f():
        return 1

    tid = f"order-{uuid.uuid4().hex}"

    def enqueue_in_new_loop():
        async def go():
            return await f.options(task_id=tid).enqueue()

        return asyncio.run(go()).id

    assert await asyncio.to_thread(enqueue_in_new_loop) == tid
    async with running(app):
        assert await app.get_result(tid, timeout=10) == 1


async def test_queue_used_from_two_event_loops_concurrently(redis_app_factory):
    """Each event loop gets its own connections; no client churn between loops."""
    app = redis_app_factory("reliable")

    @app.task
    async def echo(x):
        return x

    def other_loop():
        async def go():
            ids = [(await echo.enqueue(i)).id for i in range(50)]
            await app.close()
            return ids

        return asyncio.run(go())

    async with running(app):
        mine = [(await echo.enqueue(i)).id for i in range(50)]
        theirs = await asyncio.to_thread(other_loop)
        assert len(app.broker._conns) == 1  # the thread's loop closed its own client
        for tid in mine + theirs:
            await app.get_result(tid, timeout=10)


async def test_worker_rejects_unknown_queue_concurrency(redis_app: Queue):
    with pytest.raises(ValueError):
        Worker(redis_app, queues=["a"], queue_concurrency={"b": 1})


def test_unique_ns_helper():
    assert unique_ns() != unique_ns()
