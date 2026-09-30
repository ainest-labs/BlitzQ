"""Failure-mode tests: worker crashes, connection loss, ack failures, poison messages.

These tests use a real Redis and, for crashes, real worker subprocesses that
are killed abruptly (SIGKILL / TerminateProcess), not a simulated shutdown.
"""

import asyncio
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest
import redis
import redis.asyncio as aioredis

from blitzq import Queue, TaskState
from blitzq.broker.base import Delivery
from conftest import REDIS_URL, running, unique_ns, wait_for

pytestmark = pytest.mark.redis

HERE = Path(__file__).parent


def start_worker(ns: str, mode: str, concurrency: int = 20, vis: float = 2.0) -> subprocess.Popen:
    env = {
        **os.environ,
        "BQ_NS": ns,
        "BQ_URL": REDIS_URL,
        "BQ_MODE": mode,
        "BQ_VIS": str(vis),
        "PYTHONPATH": os.pathsep.join([str(HERE), os.environ.get("PYTHONPATH", "")]),
    }
    return subprocess.Popen(
        [sys.executable, "-m", "blitzq", "worker", "crash_app:app", "-c", str(concurrency),
         "--log-level", "WARNING"],
        cwd=HERE,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )  # fmt: skip


async def counts(r: aioredis.Redis, ns: str) -> tuple[list[int], list[int]]:
    started = [int(x) for x in await r.lrange(f"{ns}:started", 0, -1)]
    done = [int(x) for x in await r.lrange(f"{ns}:done", 0, -1)]
    return started, done


@pytest.mark.parametrize("mode", ["reliable", "fast"])
async def test_worker_crash(mode: str):
    """Kill a worker mid-execution, start a replacement, count lost/duplicate executions."""
    ns = unique_ns()
    n = 60
    producer = Queue(redis_url=REDIS_URL, mode=mode, namespace=ns)  # type: ignore[arg-type]
    r = aioredis.Redis.from_url(REDIS_URL)
    procs = []
    try:
        await producer.send("crash.work", args=[-1, 0])  # warm-up: proves the worker is up
        p1 = start_worker(ns, mode, concurrency=20)
        procs.append(p1)
        await wait_for(lambda: _done_contains(r, ns, -1), timeout=30)
        for i in range(n):
            await producer.send("crash.work", args=[i, 1.0])
        await wait_for(lambda: _started_at_least(r, ns, 21), timeout=15)
        p1.kill()  # abrupt: no graceful shutdown, no acks
        p1.wait(10)
        started_before, done_before = await counts(r, ns)
        in_flight = set(started_before) - set(done_before)
        assert in_flight, "test needs tasks in flight at the moment of the crash"

        t_restart = time.monotonic()
        p2 = start_worker(ns, mode, concurrency=40)
        procs.append(p2)
        if mode == "reliable":
            await wait_for(lambda: _all_done(r, ns, n), timeout=60)
            recovery = time.monotonic() - t_restart
            started, done = await counts(r, ns)
            done_set = set(done) - {-1}
            assert done_set == set(range(n)), "reliable mode must not lose tasks"
            dups = len([d for d in done if d != -1]) - len(done_set)
            # Tasks killed mid-execution are re-executed (at-least-once).
            assert started.count(next(iter(in_flight))) >= 2
            print(f"reliable crash: in_flight={len(in_flight)} dup_completions={dups} "
                  f"recovery={recovery:.1f}s")  # fmt: skip
        else:
            # Fast mode is at-most-once: messages popped by the killed worker are lost.
            await wait_for(lambda: _queue_drained(producer), timeout=30)
            await asyncio.sleep(1.5)
            started, done = await counts(r, ns)
            done_set = set(done) - {-1}
            lost = set(range(n)) - done_set
            assert lost, "expected fast mode to lose the in-flight tasks of a killed worker"
            assert lost <= in_flight | (set(range(n)) - set(started_before))
            assert len(done) - 1 == len(done_set), "fast mode never duplicates"
            print(f"fast crash: in_flight={len(in_flight)} lost={len(lost)}")
    finally:
        for p in procs:
            if p.poll() is None:
                p.kill()
                p.wait(10)
        await producer.broker.flush_namespace()  # type: ignore[attr-defined]
        await producer.close()
        await r.delete(f"{ns}:started", f"{ns}:done")
        await r.aclose()


async def _done_contains(r, ns, i):
    return str(i).encode() in await r.lrange(f"{ns}:done", 0, -1)


async def _started_at_least(r, ns, k):
    return await r.llen(f"{ns}:started") >= k


async def _all_done(r, ns, n):
    done = {int(x) for x in await r.lrange(f"{ns}:done", 0, -1)}
    return set(range(n)) <= done


async def _queue_drained(app):
    s = (await app.queue_stats([app.name]))[0]
    return s.waiting == 0


async def test_long_task_keeps_lease_via_heartbeat(redis_app_factory):
    """A task running 4x the visibility timeout is not recovered by a second worker."""
    app = redis_app_factory("reliable", visibility_timeout=0.6)
    runs = []

    @app.task
    async def long_job():
        runs.append(1)
        await asyncio.sleep(2.5)
        return "ok"

    async with running(app, concurrency=2), running(app, concurrency=2):
        h = await long_job.enqueue()
        assert await h.result(15) == "ok"
    assert runs == [1]


async def test_redis_connection_kill_recovers(redis_app_factory, mode):
    app = redis_app_factory(mode)
    seen: list[int] = []

    @app.task
    async def rec(i):
        await asyncio.sleep(0.01)
        seen.append(i)

    admin = aioredis.Redis.from_url(REDIS_URL)
    try:
        async with running(app, concurrency=10):
            await rec.enqueue_many((i,) for i in range(100))
            await asyncio.sleep(0.2)
            # Drop every client connection (workers, producer), as a network blip would.
            await admin.client_kill_filter(_type="normal", skipme=True)
            await asyncio.sleep(0.3)
            await rec.enqueue_many((i,) for i in range(100, 200))
            await wait_for(lambda: len(set(seen) & set(range(100, 200))) == 100, timeout=30)
            if mode == "reliable":
                # Nothing is lost; in-flight messages may need the visibility
                # timeout to be recovered, so allow for that.
                await wait_for(lambda: set(range(200)) <= set(seen), timeout=90)
    finally:
        await admin.aclose()


async def test_redis_unavailable_briefly(redis_app_factory, mode):
    """Redis blocks all commands for 1.5s (DEBUG SLEEP); the worker resumes afterwards.

    ``DEBUG SLEEP`` requires ``enable-debug-command`` (immutable: it can only be
    set at server startup, never via ``CONFIG SET``), which docker-compose's
    Redis service enables but a plain ``redis:*`` image - e.g. GitHub Actions'
    ``services:`` container, which has no way to pass startup args - does not.
    Skip rather than fail when the server refuses it.
    """
    app = redis_app_factory(mode)

    @app.task
    async def ping(i):
        return i

    admin = aioredis.Redis.from_url(REDIS_URL)
    try:
        try:
            await admin.execute_command("DEBUG", "SLEEP", "0")  # preflight: cheap, no delay
        except redis.ResponseError as exc:
            if "DEBUG command not allowed" not in str(exc):
                raise
            pytest.skip("Redis server has enable-debug-command disabled")
        async with running(app):
            assert await (await ping.enqueue(1)).result(10) == 1
            sleeper = asyncio.create_task(admin.execute_command("DEBUG", "SLEEP", "1.5"))
            await asyncio.sleep(0.1)
            h = await ping.enqueue(2)  # blocks until Redis responds again
            await sleeper
            assert await h.result(15) == 2
    finally:
        await admin.aclose()


async def test_ack_failures_are_retried(redis_app_factory):
    app = redis_app_factory("reliable")
    runs = []

    @app.task
    async def job():
        runs.append(1)
        return "ok"

    broker = app.broker
    real_complete = broker.complete
    failures = {"left": 3}

    async def flaky_complete(completions):
        if failures["left"] > 0:
            failures["left"] -= 1
            raise ConnectionError("simulated ack failure")
        await real_complete(completions)

    broker.complete = flaky_complete  # type: ignore[method-assign]
    async with running(app):
        assert await (await job.enqueue()).result(15) == "ok"
    stats = (await app.queue_stats([app.name]))[0]
    assert runs == [1] and stats.in_progress == 0 and stats.waiting == 0


async def test_lost_ack_leads_to_redelivery(redis_app_factory):
    """If a worker dies after executing but before its ack reaches Redis, the task runs again."""
    app = redis_app_factory("reliable", visibility_timeout=0.5)
    runs = []

    @app.task
    async def job():
        runs.append(1)
        return len(runs)

    real_complete = app.broker.complete

    async def never(completions):
        raise ConnectionError("ack lost")

    app.broker.complete = never  # type: ignore[method-assign]
    async with running(app) as w:
        h = await job.enqueue()
        await wait_for(lambda: runs == [1])
        w._flush_deadline = 0  # give up flushing quickly at shutdown
    app.broker.complete = real_complete  # type: ignore[method-assign]
    async with running(app, heartbeat_interval=0.2):
        assert await h.result(15) == 2  # executed twice: at-least-once
    assert runs == [1, 1]


async def test_poison_message_dead_lettered_after_max_deliveries(redis_app_factory):
    app = redis_app_factory("reliable", visibility_timeout=0.2, max_deliveries=2)

    @app.task
    async def job():
        return "never reached on first deliveries"

    broker = app.broker
    await broker.prepare_consumer([app.name], "ghost-1")
    h = await job.enqueue()
    # Two consumers take the message and "crash" (never ack).
    assert await broker.fetch(app.name, 1, 0, "ghost-1")
    await asyncio.sleep(0.3)
    got = await broker.recover(app.name, "ghost-2", 0.2, 10)
    assert len(got) == 1 and got[0].delivery_count == 2
    await asyncio.sleep(0.3)
    async with running(app, heartbeat_interval=0.05):
        await wait_for(lambda: _is_dead(app, h.id), timeout=10)
    dead = await app.dead_letters()
    assert dead[0].reason == "max deliveries exceeded"


async def _is_dead(app, tid):
    return await app.status(tid) == TaskState.DEAD_LETTERED


async def test_heartbeat_reports_lost_ownership(redis_app_factory):
    app = redis_app_factory("reliable")
    broker = app.broker
    await broker.prepare_consumer([app.name], "a")

    @app.task
    async def job():
        pass

    await job.enqueue()
    [d] = await broker.fetch(app.name, 1, 0, "a")
    assert await broker.heartbeat(app.name, [d], "a") == []
    await asyncio.sleep(0.05)
    [stolen] = await broker.recover(app.name, "b", 0.01, 1)
    assert stolen.receipt == d.receipt
    lost = await broker.heartbeat(app.name, [d], "a")
    assert [x.receipt for x in lost] == [d.receipt]
    assert isinstance(lost[0], Delivery)


async def test_graceful_shutdown_requeues_after_timeout(redis_app_factory, mode):
    app = redis_app_factory(mode)
    runs = []

    @app.task
    async def slow():
        runs.append(1)
        await asyncio.sleep(30 if len(runs) == 1 else 0)
        return "second run"

    async with running(app, shutdown_timeout=0.2):
        h = await slow.enqueue()
        await wait_for(lambda: runs == [1])
    async with running(app):
        assert await h.result(10) == "second run"
