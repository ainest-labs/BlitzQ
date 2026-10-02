"""Dead-letter filtering and bulk operations against a real Redis, in both modes."""

import asyncio
import time

import pytest

from blitzq import Queue
from conftest import running, wait_for

pytestmark = pytest.mark.redis


class GatewayTimeout(Exception): ...


class CardDeclined(Exception): ...


async def _dead_count(app: Queue) -> int:
    return await app.broker.dead_letter_count()


async def _fill(app: Queue):
    state = {"fail": True, "runs": 0}

    @app.task(name="bill.charge", retries=0)
    async def charge(i):
        if state["fail"]:
            raise GatewayTimeout(f"timeout {i}")
        state["runs"] += 1

    @app.task(name="bill.refund", retries=0)
    async def refund(i):
        raise CardDeclined("declined")

    for i in range(6):
        await charge.options(headers={"country": "IN" if i < 3 else "US"}).enqueue(i)
    for i in range(2):
        await refund.options(headers={"country": "IN"}, rate_key="stripe:IN").enqueue(i)
    async with running(app, concurrency=2):

        async def all_dead():
            return await _dead_count(app) == 8

        await wait_for(all_dead, timeout=15)
    return state


async def test_filters_pagination_and_summary(redis_app: Queue):
    await _fill(redis_app)
    assert await redis_app.count_dead_letters() == 8
    assert await redis_app.count_dead_letters(task="bill.*") == 8
    assert await redis_app.count_dead_letters(error_type="CardDeclined") == 2
    assert await redis_app.count_dead_letters(headers={"country": "IN"}) == 5
    assert await redis_app.count_dead_letters(rate_key="stripe:IN") == 2
    page = await redis_app.dead_letters(task="bill.charge", limit=2, offset=1)
    assert len(page) == 2
    assert await redis_app.dead_letter_summary() == {"GatewayTimeout": 6, "CardDeclined": 2}
    assert await redis_app.dead_letter_summary(by="header:country") == {"IN": 5, "US": 3}


async def test_time_window_uses_the_failure_time(redis_app: Queue):
    await _fill(redis_app)
    now = time.time()
    assert await redis_app.count_dead_letters(failed_after=now - 60) == 8
    assert await redis_app.count_dead_letters(failed_after=now + 60) == 0
    assert await redis_app.count_dead_letters(failed_before=now - 60) == 0
    assert await redis_app.count_dead_letters(failed_after=now - 60, failed_before=now + 60) == 8


async def test_bulk_retry_reruns_only_the_matching_tasks(redis_app: Queue):
    state = await _fill(redis_app)
    state["fail"] = False
    dry = await redis_app.retry_dead_letters(
        task="bill.charge", headers={"country": "IN"}, dry_run=True
    )
    assert dry.matched == 3 and await _dead_count(redis_app) == 8
    result = await redis_app.retry_dead_letters(task="bill.charge", headers={"country": "IN"})
    assert (result.matched, result.requeued) == (3, 3)
    async with running(redis_app, concurrency=2):
        await wait_for(lambda: state["runs"] == 3, timeout=15)
    assert await _dead_count(redis_app) == 5


async def test_two_concurrent_bulk_retries_publish_each_entry_once(redis_app: Queue):
    state = await _fill(redis_app)
    state["fail"] = False
    a, b = await asyncio.gather(
        redis_app.retry_dead_letters(task="bill.charge"),
        redis_app.retry_dead_letters(task="bill.charge"),
    )
    assert a.requeued + b.requeued == 6
    async with running(redis_app, concurrency=4):
        await wait_for(lambda: state["runs"] >= 6, timeout=15)
        await asyncio.sleep(0.5)
    assert state["runs"] == 6  # not 12


async def test_selective_purge(redis_app: Queue):
    await _fill(redis_app)
    assert await redis_app.purge_dead_letters(error_type="CardDeclined", dry_run=True) == 2
    assert await _dead_count(redis_app) == 8
    assert await redis_app.purge_dead_letters(error_type="CardDeclined") == 2
    assert await _dead_count(redis_app) == 6
    assert await redis_app.purge_dead_letters() == 6
    assert await _dead_count(redis_app) == 0


async def test_large_dead_letter_set_is_handled_in_chunks(redis_app: Queue):
    """More entries than one scan chunk, so chunking and the id snapshot are exercised."""

    @redis_app.task(name="bulk.boom", retries=0)
    async def boom(i):
        raise ValueError(f"fail {i}")

    n = 450
    await boom.enqueue_many([(i,) for i in range(n)])
    async with running(redis_app, concurrency=50):

        async def all_dead():
            return await _dead_count(redis_app) == n

        await wait_for(all_dead, timeout=60)
    # "fail 4" matches 4, 40-49 and 400-449.
    assert await redis_app.count_dead_letters(error_contains="fail 4") == 1 + 10 + 50
    result = await redis_app.retry_dead_letters(limit=300)
    assert (result.matched, result.requeued) == (300, 300)
    assert await _dead_count(redis_app) == n - 300
