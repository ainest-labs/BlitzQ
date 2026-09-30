"""Worker semantics against the in-memory broker (no external services)."""

import asyncio
import threading
import time

import pytest

from blitzq import (
    ConfigurationError,
    Queue,
    Retry,
    RetryPolicy,
    TaskFailed,
    TaskState,
    current_task,
)
from blitzq.broker import MemoryBroker
from blitzq.broker.base import PublishRequest
from conftest import running, wait_for

FAST_RETRY = RetryPolicy(initial_delay=0.01, max_delay=0.05, jitter=False)


async def test_async_and_sync_tasks(memory_app: Queue):
    @memory_app.task
    async def add(a, b):
        return a + b

    thread_names = []

    @memory_app.task
    def mul(a, b):
        thread_names.append(threading.current_thread().name)
        return a * b

    async with running(memory_app):
        h1 = await add.enqueue(2, 3)
        h2 = await mul.enqueue(4, b=5)
        assert await h1.result(5) == 5
        assert await h2.result(5) == 20
    assert thread_names and thread_names[0].startswith("blitzq-task")
    assert threading.main_thread().name not in thread_names


async def test_direct_call_runs_locally(memory_app: Queue):
    @memory_app.task
    def square(x):
        return x * x

    assert square(3) == 9


async def test_enqueue_many(memory_app: Queue):
    seen = []

    @memory_app.task
    async def record(i):
        seen.append(i)

    async with running(memory_app):
        handles = await record.enqueue_many((i,) for i in range(50))
        assert len({h.id for h in handles}) == 50
        await wait_for(lambda: len(seen) == 50)
    assert sorted(seen) == list(range(50))


async def test_global_concurrency_limit(memory_app: Queue):
    active = 0
    peak = 0
    done = 0

    @memory_app.task
    async def slow():
        nonlocal active, peak, done
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.02)
        active -= 1
        done += 1

    async with running(memory_app, concurrency=4):
        await slow.enqueue_many(() for _ in range(40))
        await wait_for(lambda: done == 40, timeout=10)
    assert peak == 4


async def test_per_queue_concurrency_and_isolation(memory_app: Queue):
    active = {"images": 0, "emails": 0}
    peak = {"images": 0, "emails": 0}
    done = {"images": 0, "emails": 0}

    @memory_app.task(queue="images")
    async def resize():
        active["images"] += 1
        peak["images"] = max(peak["images"], active["images"])
        await asyncio.sleep(0.05)
        active["images"] -= 1
        done["images"] += 1

    @memory_app.task(queue="emails")
    async def notify():
        done["emails"] += 1

    async with running(
        memory_app, queues=["images", "emails"], concurrency=10, queue_concurrency={"images": 2}
    ):
        await resize.enqueue_many(() for _ in range(40))  # large backlog
        await asyncio.sleep(0.05)
        t0 = time.monotonic()
        await notify.enqueue_many(() for _ in range(20))
        await wait_for(lambda: done["emails"] == 20, timeout=5)
        emails_latency = time.monotonic() - t0
        # 40 images at 2-at-a-time x 50ms = ~1s; emails must not wait for that backlog.
        assert done["images"] < 40
        assert emails_latency < 0.5
    assert peak["images"] == 2


async def test_backpressure_leaves_backlog_in_broker(memory_app: Queue):
    gate = asyncio.Event()

    @memory_app.task
    async def blocked():
        await gate.wait()

    async with running(memory_app, concurrency=3):
        await blocked.enqueue_many(() for _ in range(20))
        await asyncio.sleep(0.2)
        stats = await memory_app.queue_stats(["default"])
        assert stats[0].waiting == 17  # only 3 taken from the broker
        gate.set()
        await wait_for(lambda: len(memory_app.broker._queues["default"]) == 0)  # type: ignore[attr-defined]


async def test_async_timeout_and_retry_then_dead_letter(memory_app: Queue):
    attempts = []

    @memory_app.task(timeout=0.05, retries=2, retry_policy=FAST_RETRY)
    async def hang():
        attempts.append(current_task().attempt)  # type: ignore[union-attr]
        await asyncio.sleep(10)

    async with running(memory_app):
        h = await hang.enqueue()
        with pytest.raises(TaskFailed) as ei:
            await h.result(5)
    assert attempts == [1, 2, 3]
    assert ei.value.state == "dead_lettered"
    assert ei.value.error_type == "TaskTimeout"
    dead = await memory_app.dead_letters()
    assert [d.id for d in dead] == [h.id]
    assert dead[0].reason == "max attempts exceeded" and dead[0].attempt == 3


async def test_thread_timeout_keeps_slot_until_thread_returns(memory_app: Queue):
    release = threading.Event()

    @memory_app.task(timeout=0.05)
    def stuck():
        release.wait(5)

    async with running(memory_app, concurrency=1) as w:
        h = await stuck.enqueue()
        with pytest.raises(TaskFailed) as ei:
            await h.result(5)
        assert ei.value.error_type == "TaskTimeout"
        # The thread is still running, so the only slot remains occupied.
        assert w.global_limit.used == 1
        release.set()
        await wait_for(lambda: w.global_limit.used == 0)


async def test_non_retryable_exception_fails_immediately(memory_app: Queue):
    calls = 0

    @memory_app.task(retries=5, retry_policy=RetryPolicy(retry_on=(ConnectionError,)))
    async def bad():
        nonlocal calls
        calls += 1
        raise ValueError("permanent")

    async with running(memory_app):
        h = await bad.enqueue()
        with pytest.raises(TaskFailed):
            await h.result(5)
    assert calls == 1
    assert (await memory_app.dead_letters())[0].reason == "non-retryable error"


async def test_explicit_retry_counts_toward_attempts(memory_app: Queue):
    attempts = []

    @memory_app.task(retries=3, retry_policy=RetryPolicy(retry_on=()))
    async def poll():
        ctx = current_task()
        assert ctx is not None
        attempts.append(ctx.attempt)
        if ctx.attempt < 3:
            raise Retry(delay=0.01)
        return f"done after {ctx.retries} retries"

    async with running(memory_app):
        assert await (await poll.enqueue()).result(5) == "done after 2 retries"
    assert attempts == [1, 2, 3]


async def test_explicit_retry_cannot_loop_forever(memory_app: Queue):
    calls = 0

    @memory_app.task(retries=2)
    async def forever():
        nonlocal calls
        calls += 1
        raise Retry(delay=0)

    async with running(memory_app):
        with pytest.raises(TaskFailed):
            await (await forever.enqueue()).result(5)
    assert calls == 3


async def test_retry_backoff_is_scheduled_not_slept(memory_app: Queue):
    """While a task waits for its retry, the worker keeps processing other tasks."""
    order = []

    @memory_app.task(retries=1, retry_policy=RetryPolicy(initial_delay=0.3, jitter=False))
    async def flaky():
        order.append("flaky")
        if len(order) == 1:
            raise ConnectionError

    @memory_app.task
    async def quick(i):
        order.append(i)

    async with running(memory_app, concurrency=1):
        h = await flaky.enqueue()
        await asyncio.sleep(0.05)
        await quick.enqueue_many((i,) for i in range(3))
        await h.result(5)
    assert order == ["flaky", 0, 1, 2, "flaky"]


async def test_dead_letter_disabled_marks_failed(memory_app: Queue):
    @memory_app.task(dead_letter=False)
    async def bad():
        raise RuntimeError("x")

    async with running(memory_app):
        h = await bad.enqueue()
        with pytest.raises(TaskFailed) as ei:
            await h.result(5)
    assert ei.value.state == "failed"
    assert await memory_app.dead_letters() == []


async def test_dead_letter_replay(memory_app: Queue):
    fail = True

    @memory_app.task
    async def sometimes():
        if fail:
            raise RuntimeError("down")
        return "ok"

    async with running(memory_app):
        h = await sometimes.enqueue()
        with pytest.raises(TaskFailed):
            await h.result(5)
        fail = False
        assert await memory_app.retry(h.id) is True
        assert await memory_app.retry(h.id) is False  # already replayed
        await wait_for(lambda: memory_app.status(h.id), timeout=5)
        assert await h.result(5) == "ok"


async def test_unknown_task_and_malformed_messages_are_dead_lettered(memory_app: Queue):
    async with running(memory_app):
        await memory_app.send("does.not.exist", args=[1])
        await memory_app.broker.publish([PublishRequest("default", "x", b"\x00garbage")])
        await wait_for(lambda: memory_app.broker.dead_letter_count(), timeout=5)
        await wait_for(lambda: len(memory_app.broker._dlq) == 2, timeout=5)  # type: ignore[attr-defined]
    reasons = sorted(d.reason for d in await memory_app.dead_letters())
    assert reasons == ["malformed message", "unknown task"]


async def test_unserializable_result_marks_failed(memory_app: Queue):
    @memory_app.task
    async def weird():
        return object()

    async with running(memory_app):
        with pytest.raises(TaskFailed) as ei:
            await (await weird.enqueue()).result(5)
    assert ei.value.error_type == "SerializationError"


async def test_store_result_disabled(memory_app: Queue):
    ran = asyncio.Event()

    @memory_app.task(store_result=False)
    async def fire_and_forget():
        ran.set()

    async with running(memory_app):
        h = await fire_and_forget.enqueue()
        await asyncio.wait_for(ran.wait(), 5)
        await asyncio.sleep(0.05)
    assert await memory_app.inspect(h.id) is None


async def test_track_state_transitions():
    app = Queue(broker=MemoryBroker(), track_state=True)
    gate = asyncio.Event()

    @app.task
    async def job():
        await gate.wait()
        return 1

    h = await job.enqueue()
    assert await h.status() == TaskState.QUEUED
    async with running(app):
        await wait_for(lambda: _is(app, h.id, TaskState.RUNNING))
        gate.set()
        assert await h.result(5) == 1
    info = await h.info()
    assert info is not None and info.state == TaskState.SUCCEEDED
    assert info.started_at is not None and info.duration is not None and info.duration >= 0
    assert info.worker is not None


async def _is(app, task_id, state):
    return await app.status(task_id) == state


async def test_delayed_task_and_scheduled_status(memory_app: Queue):
    @memory_app.task
    async def later():
        return time.time()

    async with running(memory_app):
        t0 = time.time()
        h = await later.options(delay=0.3).enqueue()
        assert await h.status() == TaskState.SCHEDULED
        ran_at = await h.result(5)
    assert ran_at - t0 >= 0.29


async def test_cancel_scheduled_task(memory_app: Queue):
    ran = []

    @memory_app.task
    async def later():
        ran.append(1)

    async with running(memory_app):
        h = await later.options(delay=0.2).enqueue()
        assert await h.cancel() is True
        await asyncio.sleep(0.4)
        with pytest.raises(TaskFailed) as ei:
            await h.result(1)
    assert ran == [] and ei.value.state == "cancelled"


async def test_revoke_queued_task(memory_app: Queue):
    ran = []

    @memory_app.task
    async def job():
        ran.append(1)

    h = await job.enqueue()
    assert await memory_app.cancel(h.id) is False  # not scheduled: revocation recorded
    async with running(memory_app):
        with pytest.raises(TaskFailed) as ei:
            await h.result(5)
    assert ran == [] and ei.value.state == "cancelled"


async def test_revoke_running_async_task(memory_app: Queue):
    started = asyncio.Event()

    @memory_app.task
    async def long():
        started.set()
        await asyncio.sleep(30)

    async with running(memory_app):
        h = await long.enqueue()
        await asyncio.wait_for(started.wait(), 5)
        await memory_app.cancel(h.id)
        with pytest.raises(TaskFailed) as ei:
            await h.result(5)
    assert ei.value.state == "cancelled"


async def test_context_and_correlation_propagation(memory_app: Queue):
    seen = {}

    @memory_app.task
    async def child():
        ctx = current_task()
        seen["child"] = (ctx.correlation_id, ctx.headers)  # type: ignore[union-attr]

    @memory_app.task
    def parent_sync():
        ctx = current_task()  # context is propagated into the executor thread
        seen["parent"] = (ctx.name, ctx.attempt, ctx.correlation_id)  # type: ignore[union-attr]
        child.enqueue_sync()

    @memory_app.task
    async def parent():
        await child.enqueue()

    async with running(memory_app):
        await parent.options(correlation_id="req-42", headers={"traceparent": "00-abc"}).enqueue()
        await wait_for(lambda: "child" in seen)
    assert seen["child"] == ("req-42", {"traceparent": "00-abc"})
    assert current_task() is None


async def test_graceful_shutdown_waits_for_running_tasks(memory_app: Queue):
    finished = []

    @memory_app.task
    async def work():
        await asyncio.sleep(0.2)
        finished.append(1)

    async with running(memory_app):
        await work.enqueue()
        await asyncio.sleep(0.05)
    assert finished == [1]


async def test_shutdown_timeout_requeues_unfinished_tasks(memory_app: Queue):
    @memory_app.task
    async def slow():
        await asyncio.sleep(30)

    async with running(memory_app, shutdown_timeout=0.1):
        await slow.enqueue()
        await asyncio.sleep(0.1)
    # Cancelled at shutdown and returned to the queue for another worker.
    assert (await memory_app.queue_stats(["default"]))[0].waiting == 1


async def test_sync_api_refuses_to_block_running_loop(memory_app: Queue):
    @memory_app.task
    async def t():
        pass

    with pytest.raises(RuntimeError, match="running event loop"):
        t.enqueue_sync()


def test_registration_errors():
    app = Queue(broker=MemoryBroker())

    @app.task(name="x")
    def f():
        pass

    with pytest.raises(ConfigurationError):
        app.task(name="x")(lambda: None)
    with pytest.raises(ConfigurationError):

        @app.task(executor="thread")
        async def g():
            pass

    with pytest.raises(ConfigurationError):

        @app.task(executor="async")
        def h():
            pass

    with pytest.raises(ConfigurationError):
        app.task(retries=-1)(lambda: None)


def test_default_queue_routing_and_names():
    app = Queue("main", broker=MemoryBroker(), routes={"*.build_report*": "reports"})

    @app.task
    def build_report():
        pass

    @app.task(queue="emails")
    def build_report_email():
        pass

    @app.task
    def other():
        pass

    assert build_report.name.endswith("test_default_queue_routing_and_names.<locals>.build_report")
    assert build_report.queue == "reports"
    assert build_report_email.queue == "emails"
    assert other.queue == "main"


async def test_options_eta_requires_aware_datetime(memory_app: Queue):
    from datetime import datetime

    @memory_app.task
    async def t():
        pass

    with pytest.raises(ConfigurationError):
        t.options(eta=datetime(2030, 1, 1))
    with pytest.raises(ConfigurationError):
        t.options(delay=1, eta=1.0)
