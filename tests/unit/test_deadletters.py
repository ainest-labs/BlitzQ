"""Dead-letter filtering, bulk requeue, selective purge and summaries."""

import asyncio
import time

import pytest

from blitzq import (
    BulkResult,
    ConfigurationError,
    DeadLetterFilter,
    Queue,
    RetryPolicy,
    current_task,
)
from blitzq.serialization import DeadLetter
from conftest import running, wait_for


class GatewayTimeout(Exception): ...


class CardDeclined(Exception): ...


class Flags:
    fail = True


@pytest.fixture
def billing(memory_app: Queue):
    """A queue with a failing billing task, a failing refund task and a succeeding replay log."""
    Flags.fail = True
    attempts: list[tuple[str, int]] = []

    @memory_app.task(name="billing.charge", retries=0)
    async def charge(i):
        ctx = current_task()
        if Flags.fail:
            raise GatewayTimeout(f"gateway timed out for order {i}")
        attempts.append((f"charge-{i}", ctx.attempt))
        return i

    @memory_app.task(name="billing.refund", retries=0)
    async def refund(i):
        raise CardDeclined(f"card declined for refund {i}")

    @memory_app.task(name="notify.email", retries=0, queue="mail")
    async def email(i):
        raise ValueError("smtp refused")

    memory_app.attempts = attempts  # type: ignore[attr-defined]
    return memory_app, charge, refund, email


async def _fill(app, charge, refund, email):
    """6 charges (IN x3, US x3), 2 refunds (IN), 1 email, dead-lettered in that order."""
    for i in range(6):
        country = "IN" if i < 3 else "US"
        await charge.options(headers={"country": country}).enqueue(i)
    for i in range(2):
        await refund.options(headers={"country": "IN"}, rate_key="stripe:IN").enqueue(i)
    await email.options(correlation_id="corr-1").enqueue(0)
    async with running(app, concurrency=1):
        await wait_for(lambda: app.broker._dlq and len(app.broker._dlq) == 9, timeout=10)


async def test_filters(billing):
    app, charge, refund, email = billing
    await _fill(app, charge, refund, email)

    async def ids(**f):
        return {d.task + ":" + d.id[:0] for d in await app.dead_letters(**f)}, len(
            await app.dead_letters(**f)
        )

    assert await app.count_dead_letters() == 9
    assert await app.count_dead_letters(task="billing.charge") == 6
    assert await app.count_dead_letters(task="billing.*") == 8
    assert await app.count_dead_letters(error_type="CardDeclined") == 2
    assert await app.count_dead_letters(error_contains="TIMED OUT") == 6
    assert await app.count_dead_letters(error_contains="smtp") == 1
    assert await app.count_dead_letters(queue="mail") == 1
    assert await app.count_dead_letters(headers={"country": "IN"}) == 5
    assert await app.count_dead_letters(headers={"country": "IN"}, task="billing.charge") == 3
    assert await app.count_dead_letters(rate_key="stripe:IN") == 2
    assert await app.count_dead_letters(correlation_id="corr-1") == 1
    assert await app.count_dead_letters(error_type="GatewayTimeout", headers={"country": "US"}) == 3
    assert await app.count_dead_letters(headers={"country": "FR"}) == 0
    assert (
        await app.count_dead_letters(reason="max attempts exceeded")
        + await app.count_dead_letters(reason="non-retryable error")
        == 9
    )


async def test_filter_by_time_window(billing):
    app, charge, *_ = billing
    await charge.enqueue(1)
    async with running(app):
        await wait_for(lambda: len(app.broker._dlq) == 1)
        await asyncio.sleep(0.05)
        midpoint = time.time()
        await asyncio.sleep(0.05)
        await charge.enqueue(2)
        await wait_for(lambda: len(app.broker._dlq) == 2)
    assert await app.count_dead_letters(failed_after=midpoint) == 1
    assert await app.count_dead_letters(failed_before=midpoint) == 1
    assert (
        await app.count_dead_letters(failed_after=midpoint - 60, failed_before=midpoint + 60) == 2
    )


async def test_pagination_with_filters(billing):
    app, charge, refund, email = billing
    await _fill(app, charge, refund, email)
    everything = await app.dead_letters(task="billing.charge", limit=100)
    assert len(everything) == 6
    times = [d.failed_at for d in everything]
    assert times == sorted(times, reverse=True)  # newest first
    page2 = await app.dead_letters(task="billing.charge", limit=2, offset=2)
    assert [d.id for d in page2] == [d.id for d in everything[2:4]]


async def test_unknown_filter_is_rejected(billing):
    app, *_ = billing
    with pytest.raises(TypeError):
        await app.dead_letters(colour="red")


async def test_naive_datetime_is_rejected():
    from datetime import datetime

    with pytest.raises(ConfigurationError):
        DeadLetterFilter(failed_after=datetime(2026, 1, 1))


async def test_retry_only_matching_and_attempt_budget_is_fresh(billing):
    app, charge, refund, email = billing
    await _fill(app, charge, refund, email)
    Flags.fail = False
    async with running(app, concurrency=2):
        result = await app.retry_dead_letters(task="billing.charge", headers={"country": "IN"})
        assert isinstance(result, BulkResult)
        assert (result.matched, result.requeued, result.skipped, result.unreplayable) == (
            3,
            3,
            0,
            0,
        )
        await wait_for(lambda: len(app.attempts) == 3)
    assert {a for _, a in app.attempts} == {1}
    assert await app.count_dead_letters() == 6
    assert await app.count_dead_letters(task="billing.charge") == 3
    assert await app.count_dead_letters(task="billing.charge", headers={"country": "IN"}) == 0


async def test_retry_oldest_first_and_limit(billing):
    app, charge, refund, email = billing
    await _fill(app, charge, refund, email)
    oldest = [d.id for d in reversed(await app.dead_letters(task="billing.charge", limit=100))]
    dry = await app.retry_dead_letters(task="billing.charge", limit=2, dry_run=True)
    assert dry.dry_run and dry.matched == 2 and dry.requeued == 0
    assert dry.ids == oldest[:2]
    assert await app.count_dead_letters() == 9  # a dry run changes nothing
    real = await app.retry_dead_letters(task="billing.charge", limit=2)
    assert real.requeued == 2 and real.ids == oldest[:2]
    assert await app.count_dead_letters() == 7


async def test_retry_all_with_no_filters(billing):
    app, charge, refund, email = billing
    await _fill(app, charge, refund, email)
    result = await app.retry_dead_letters()
    assert result.matched == result.requeued == 9
    assert await app.count_dead_letters() == 0


async def test_concurrent_bulk_retries_publish_each_entry_once(billing):
    app, charge, refund, email = billing
    await _fill(app, charge, refund, email)
    a, b = await asyncio.gather(app.retry_dead_letters(), app.retry_dead_letters())
    assert a.requeued + b.requeued == 9
    assert a.skipped + b.skipped == (a.matched + b.matched) - 9
    queued = sum(len(q) for q in app.broker._queues.values())
    assert queued == 9  # nothing was published twice


async def test_retry_rate_limit_paces_the_replay(billing):
    app, charge, refund, email = billing
    await _fill(app, charge, refund, email)
    started = time.monotonic()
    result = await app.retry_dead_letters(task="billing.charge", rate=12)
    elapsed = time.monotonic() - started
    assert result.requeued == 6
    assert elapsed >= 0.4  # 6 entries at 12/s


async def test_invalid_retry_arguments(billing):
    app, *_ = billing
    with pytest.raises(ConfigurationError):
        await app.retry_dead_letters(limit=0)
    with pytest.raises(ConfigurationError):
        await app.retry_dead_letters(rate=0)


async def test_undecodable_entries_are_reported_not_replayed(billing):
    app, *_ = billing
    app.broker._dlq["bad"] = (
        time.time(),
        app.serializer.encode_dead(
            DeadLetter(
                id="bad",
                queue="default",
                task="x",
                reason="malformed message",
                failed_at=time.time(),
                message=b"garbage",
            )
        ),
    )
    result = await app.retry_dead_letters()
    assert (result.matched, result.requeued, result.unreplayable) == (1, 0, 1)
    assert await app.count_dead_letters() == 1  # kept for inspection


async def test_entry_removed_by_someone_else_counts_as_skipped(billing):
    app, charge, refund, email = billing
    await _fill(app, charge, refund, email)
    real = app.broker.replay_dead_letter
    calls = {"n": 0}

    async def flaky(task_id, queue, data):
        calls["n"] += 1
        if calls["n"] == 1:
            await app.broker.delete_dead_letters([task_id])  # someone purged it first
        return await real(task_id, queue, data)

    app.broker.replay_dead_letter = flaky  # type: ignore[method-assign]
    result = await app.retry_dead_letters(task="billing.charge")
    assert result.matched == 6 and result.skipped == 1 and result.requeued == 5


async def test_selective_purge(billing):
    app, charge, refund, email = billing
    await _fill(app, charge, refund, email)
    assert await app.purge_dead_letters(task="billing.refund", dry_run=True) == 2
    assert await app.count_dead_letters() == 9
    assert await app.purge_dead_letters(task="billing.refund") == 2
    assert await app.count_dead_letters() == 7
    assert await app.purge_dead_letters(dry_run=True) == 7
    assert await app.purge_dead_letters() == 7
    assert await app.count_dead_letters() == 0


async def test_summary(billing):
    app, charge, refund, email = billing
    await _fill(app, charge, refund, email)
    assert await app.dead_letter_summary() == {
        "GatewayTimeout": 6,
        "CardDeclined": 2,
        "ValueError": 1,
    }
    assert await app.dead_letter_summary(by="task") == {
        "billing.charge": 6, "billing.refund": 2, "notify.email": 1,
    }  # fmt: skip
    assert await app.dead_letter_summary(by="header:country") == {"IN": 5, "US": 3, "-": 1}
    assert await app.dead_letter_summary(by="rate_key", task="billing.*") == {
        "-": 6,
        "stripe:IN": 2,
    }
    assert await app.dead_letter_summary(by="error_type", headers={"country": "IN"}) == {
        "GatewayTimeout": 3, "CardDeclined": 2,
    }  # fmt: skip
    for bad in ("colour", "header:"):
        with pytest.raises(ConfigurationError):
            await app.dead_letter_summary(by=bad)


async def test_replay_keeps_headers_and_correlation(billing):
    app, charge, refund, email = billing
    await _fill(app, charge, refund, email)
    await app.retry_dead_letters(correlation_id="corr-1")
    raw = next(iter(app.broker._queues["mail"]))
    env = app.serializer.decode_envelope(raw)
    assert env.correlation_id == "corr-1" and env.attempt == 1


def test_retry_dead_letters_sync(memory_app: Queue):
    @memory_app.task(retries=0)
    def boom():
        raise ValueError("x")

    boom.enqueue_sync()

    async def drain():
        async with running(memory_app):
            await wait_for(lambda: len(memory_app.broker._dlq) == 1)

    asyncio.run(drain())
    result = memory_app.retry_dead_letters_sync(task="*boom")
    assert result.requeued == 1


async def test_replay_gives_a_fresh_attempt_budget(memory_app: Queue):
    """A task that exhausted three attempts must run as attempt 1 again."""
    Flags.fail = True
    seen: list[int] = []

    @memory_app.task(
        retries=2,
        retry_policy=RetryPolicy(initial_delay=0.02, max_delay=0.02, backoff=1, jitter=False),
    )
    async def stubborn():
        ctx = current_task()
        seen.append(ctx.attempt)
        if Flags.fail:
            raise GatewayTimeout("still down")

    await stubborn.enqueue()
    async with running(memory_app, schedule_poll_interval=0.02):
        await wait_for(lambda: len(memory_app.broker._dlq) == 1)
        dead = (await memory_app.dead_letters())[0]
        assert dead.attempt == 3 and seen == [1, 2, 3]
        Flags.fail = False
        await memory_app.retry_dead_letters()
        await wait_for(lambda: len(seen) == 4)
    assert seen[-1] == 1


async def test_queue_filter_includes_priority_levels(memory_app: Queue):
    @memory_app.task(retries=0, priority="high")
    async def urgent():
        raise ValueError("x")

    @memory_app.task(retries=0)
    async def normal():
        raise ValueError("x")

    await urgent.enqueue()
    await normal.enqueue()
    async with running(memory_app):
        await wait_for(lambda: len(memory_app.broker._dlq) == 2)
    physical = {d.queue for d in await memory_app.dead_letters()}
    assert "default:high" in physical
    assert await memory_app.count_dead_letters(queue="default") == 2
    assert await memory_app.count_dead_letters(queue="default:high") == 1
