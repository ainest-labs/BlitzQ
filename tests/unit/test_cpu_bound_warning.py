"""The runtime warning that nudges a mis-placed CPU-bound task toward executor='process'."""

import asyncio
import logging
import time

from blitzq import Queue
from conftest import running, wait_for


def _busy(seconds: float) -> None:
    t0 = time.monotonic()
    while time.monotonic() - t0 < seconds:
        pass


def _crunch() -> int:
    # Module-level: executor="process" resolves tasks by import path, so a
    # closure defined inside a test function cannot be looked up in the
    # child process (see blitzq.worker._call_in_process).
    return sum(range(2_000_000))


async def test_warns_once_for_a_cpu_bound_thread_task(memory_app: Queue, caplog):
    @memory_app.task
    def spin():
        _busy(0.15)

    caplog.set_level(logging.WARNING, logger="blitzq")
    async with running(memory_app, concurrency=4):
        await spin.enqueue_many(() for _ in range(3))
        await wait_for(
            lambda: sum(1 for r in caplog.records if "looks CPU-bound" in r.message) >= 1
        )
        await asyncio.sleep(0.5)  # let the other two calls finish too
    warnings = [r for r in caplog.records if "looks CPU-bound" in r.message]
    assert len(warnings) == 1  # only once per task name, not once per call
    assert 'executor="process"' in warnings[0].message


async def test_no_warning_for_an_io_bound_thread_task(memory_app: Queue, caplog):
    @memory_app.task
    def sleepy():
        time.sleep(0.15)  # mostly blocked, not on the CPU

    caplog.set_level(logging.WARNING, logger="blitzq")
    async with running(memory_app, concurrency=2):
        await (await sleepy.enqueue()).result(5)
    assert not any("looks CPU-bound" in r.message for r in caplog.records)


async def test_no_warning_below_the_duration_threshold(memory_app: Queue, caplog):
    @memory_app.task
    def quick_spin():
        _busy(0.02)  # CPU-bound, but too short to matter

    caplog.set_level(logging.WARNING, logger="blitzq")
    async with running(memory_app, concurrency=2):
        await (await quick_spin.enqueue()).result(5)
    assert not any("looks CPU-bound" in r.message for r in caplog.records)


async def test_process_executor_never_warns(memory_app: Queue, caplog):
    crunch = memory_app.task(executor="process")(_crunch)

    caplog.set_level(logging.WARNING, logger="blitzq")
    async with running(memory_app, concurrency=2):
        await (await crunch.enqueue()).result(15)
    assert not any("looks CPU-bound" in r.message for r in caplog.records)


async def test_warn_cpu_bound_can_be_disabled(memory_app: Queue, caplog):
    @memory_app.task
    def spin():
        _busy(0.15)

    caplog.set_level(logging.WARNING, logger="blitzq")
    async with running(memory_app, concurrency=2, warn_cpu_bound=False):
        await (await spin.enqueue()).result(5)
    assert not any("looks CPU-bound" in r.message for r in caplog.records)
