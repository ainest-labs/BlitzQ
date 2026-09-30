"""Periodic scheduling: coordination, restart recovery, missed runs, clock edge cases."""

import asyncio
import time

import pytest

from blitzq import Every, Queue, TaskState
from blitzq.scheduler import Scheduler
from conftest import running, wait_for

pytestmark = pytest.mark.redis


def make(app: Queue, schedule=Every(60), missed="run_once") -> None:
    @app.periodic(schedule, missed=missed, name="report")
    async def report():
        return "ok"


async def queued(app: Queue) -> int:
    return (await app.queue_stats([app.name]))[0].waiting


async def test_first_start_does_not_backfill(redis_app: Queue):
    make(redis_app)
    s = Scheduler(redis_app)
    now = 1_000_020.0  # a multiple of 60: occurrences fall on now + 60k
    assert await s.tick(now) == 0  # baseline, no backfill
    assert await s.tick(now + 30) == 0
    assert await s.tick(now + 60) == 1
    assert await queued(redis_app) == 1


async def test_concurrent_schedulers_dispatch_each_occurrence_once(redis_app: Queue):
    make(redis_app, Every(10))
    schedulers = [Scheduler(redis_app) for _ in range(8)]
    base = 2_000_000.0
    for s in schedulers:
        await s.tick(base)
    total = 0
    for step in range(1, 6):
        results = await asyncio.gather(*(s.tick(base + 10 * step) for s in schedulers))
        assert sum(results) == 1  # exactly one winner per occurrence
        total += sum(results)
    assert total == 5
    assert await queued(redis_app) == 5


@pytest.mark.parametrize(("missed", "expected"), [("run_once", 1), ("run_all", 10), ("skip", 0)])
async def test_restart_recovery_missed_policies(redis_app: Queue, missed, expected):
    make(redis_app, Every(10), missed=missed)
    base = 3_000_000.0
    first = Scheduler(redis_app)
    await first.tick(base)
    assert await first.tick(base + 10) == 1  # dispatched occurrence base+10
    # Scheduler down; a new instance starts 5s after occurrence base+110
    # (beyond the default 2s grace), so 10 occurrences were missed.
    restarted = Scheduler(redis_app)
    assert await restarted.tick(base + 115) == expected
    last = (await redis_app.broker.periodic_last())["report"]
    assert last == base + 110  # policy decisions are recorded, even for "skip"
    assert await restarted.tick(base + 115) == 0


async def test_skip_policy_still_runs_on_time_occurrence(redis_app: Queue):
    make(redis_app, Every(10), missed="skip")
    s = Scheduler(redis_app, grace=2)
    await s.tick(4_000_000.0)
    assert await s.tick(4_000_011.0) == 1  # 1s late: within grace


async def test_clock_moving_backwards_does_not_redispatch(redis_app: Queue):
    make(redis_app, Every(10))
    s = Scheduler(redis_app)
    base = 5_000_000.0
    await s.tick(base)
    assert await s.tick(base + 20) == 1
    assert await s.tick(base + 5) == 0  # clock jumped back
    assert await s.tick(base + 20) == 0
    assert await s.tick(base + 30) == 1


async def test_catchup_window_limits_backfill(redis_app: Queue):
    make(redis_app, Every(10), missed="run_all")
    s = Scheduler(redis_app, catchup_window=50, max_catchup=100)
    await redis_app.broker.claim_periodic("report", 6_000_000.0, "default", None)
    assert await s.tick(6_001_000.0) == 5  # only occurrences within the last 50s


async def test_periodic_occurrences_execute_with_deterministic_ids(redis_app: Queue):
    runs: list[float] = []

    @redis_app.periodic(Every(0.3), name="tick")
    async def tick():
        runs.append(time.time())

    s = Scheduler(redis_app, poll_interval=0.05)
    task = asyncio.create_task(s.run())
    try:
        async with running(redis_app):
            await wait_for(lambda: len(runs) >= 3, timeout=10)
    finally:
        s.stop()
        await task
    last = (await redis_app.broker.periodic_last())["tick"]
    occurrence_id = f"periodic:tick:{last:.3f}".rstrip("0").rstrip(".")
    await wait_for(lambda: _done(redis_app, occurrence_id), timeout=5)


async def _done(app, tid):
    return await app.status(tid) == TaskState.SUCCEEDED


async def test_scheduler_promotes_delayed_tasks_without_workers_promoting(redis_app: Queue):
    @redis_app.task
    async def later():
        return "done"

    s = Scheduler(redis_app, poll_interval=0.05)
    task = asyncio.create_task(s.run())
    try:
        h = await later.options(delay=0.2).enqueue()
        await wait_for(lambda: _waiting_is(redis_app, 1), timeout=5)
        async with running(redis_app, promote=False):
            assert await h.result(10) == "done"
    finally:
        s.stop()
        await task


async def _waiting_is(app, n):
    return await queued(app) == n


def test_duplicate_periodic_registration():
    from blitzq.broker import MemoryBroker
    from blitzq.exceptions import ConfigurationError

    app = Queue(broker=MemoryBroker())

    @app.periodic(10, name="x")
    def f():
        pass

    with pytest.raises(ConfigurationError):
        app.periodic(10)(f)
