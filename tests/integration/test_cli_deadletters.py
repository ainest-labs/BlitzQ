"""Dead-letter CLI commands against a real Redis (sync tests: the CLI runs its own loop)."""

import asyncio
import json

import pytest
import redis
from typer.testing import CliRunner

from blitzq import Queue
from blitzq.cli import app as cli
from conftest import REDIS_URL, running, unique_ns, wait_for

pytestmark = pytest.mark.redis
runner = CliRunner()


class GatewayTimeout(Exception): ...


class CardDeclined(Exception): ...


@pytest.fixture
def ns():
    ns = unique_ns()
    yield ns
    r = redis.Redis.from_url(REDIS_URL)
    for key in r.scan_iter(f"{ns}:*"):
        r.delete(key)


def conn(ns: str) -> list[str]:
    return ["--redis-url", REDIS_URL, "--namespace", ns, "--mode", "fast"]


def invoke(*args: str, ok: bool = True):
    res = runner.invoke(cli, list(args), catch_exceptions=False)
    if ok:
        assert res.exit_code == 0, res.stdout
    return res


@pytest.fixture
def dead(ns):
    """4 charge failures (IN x2, US x2) and 1 refund failure (IN), all dead-lettered."""
    app = Queue(redis_url=REDIS_URL, namespace=ns, mode="fast")

    @app.task(name="bill.charge", retries=0)
    async def charge(i):
        raise GatewayTimeout(f"timeout {i}")

    @app.task(name="bill.refund", retries=0)
    async def refund(i):
        raise CardDeclined("declined")

    async def go():
        for i in range(4):
            await charge.options(headers={"country": "IN" if i < 2 else "US"}).enqueue(i)
        await refund.options(headers={"country": "IN"}).enqueue(0)
        async with running(app):

            async def five():
                return await app.broker.dead_letter_count() == 5

            await wait_for(five, timeout=15)
        await app.close()

    asyncio.run(go())
    return ns


def total(ns: str, *extra: str) -> int:
    out = invoke("dead-letter", "list", *conn(ns), *extra, "--json").stdout
    return int(json.loads(out)["total"])


def test_list_filters(dead):
    assert total(dead) == 5
    assert total(dead, "--task", "bill.charge") == 4
    assert total(dead, "--error-type", "CardDeclined") == 1
    assert total(dead, "--header", "country=IN") == 3
    assert total(dead, "--task", "bill.charge", "-H", "country=US") == 2
    assert total(dead, "--error-contains", "TIMEOUT") == 4
    assert total(dead, "--since", "1h") == 5
    assert total(dead, "--until", "1h") == 0
    res = invoke("dead-letter", "list", *conn(dead), "--task", "bill.refund")
    assert "1 dead-lettered task(s) matching" in res.stdout and "CardDeclined" in res.stdout


def test_list_pagination_reports_the_filtered_total(dead):
    out = json.loads(
        invoke(
            "dead-letter", "list", *conn(dead), "--task", "bill.charge", "--limit", "1", "--json"
        ).stdout
    )
    assert out["total"] == 4 and len(out["items"]) == 1


def test_summary(dead):
    out = json.loads(invoke("dead-letter", "summary", *conn(dead), "--json").stdout)
    assert out["total"] == 5 and out["groups"] == {"GatewayTimeout": 4, "CardDeclined": 1}
    out = json.loads(
        invoke("dead-letter", "summary", *conn(dead), "--by", "header:country", "--json").stdout
    )
    assert out["groups"] == {"IN": 3, "US": 2}
    res = invoke("dead-letter", "summary", *conn(dead), "--by", "task")
    assert "bill.charge" in res.stdout and "4" in res.stdout
    assert invoke("dead-letter", "summary", *conn(dead), "--by", "colour", ok=False).exit_code == 2


def test_retry_all_requires_confirmation_and_supports_dry_run(dead):
    refused = invoke("dead-letter", "retry-all", *conn(dead), ok=False)
    assert refused.exit_code == 2 and total(dead) == 5
    dry = invoke("dead-letter", "retry-all", *conn(dead), "--dry-run", "--task", "bill.refund")
    assert "would re-enqueue 1" in dry.stdout and total(dead) == 5


def test_retry_all_replays_only_matching(dead):
    res = invoke("dead-letter", "retry-all", *conn(dead), "--yes", "-H", "country=US")
    assert "re-enqueued 2 of 2 matching" in res.stdout
    assert total(dead) == 3
    res = invoke("dead-letter", "retry-all", *conn(dead), "--yes", "--limit", "1", "--rate", "50")
    assert "re-enqueued 1 of 1" in res.stdout
    assert total(dead) == 2


def test_retry_all_json(dead):
    out = json.loads(
        invoke(
            "dead-letter", "retry-all", *conn(dead), "--yes", "--task", "bill.refund", "--json"
        ).stdout
    )
    assert out["matched"] == 1 and out["requeued"] == 1 and len(out["ids"]) == 1


def test_selective_purge(dead):
    refused = invoke("dead-letter", "purge", *conn(dead), "--error-type", "CardDeclined", ok=False)
    assert refused.exit_code == 2 and total(dead) == 5
    dry = invoke("dead-letter", "purge", *conn(dead), "--dry-run", "--error-type", "CardDeclined")
    assert "would delete 1" in dry.stdout and total(dead) == 5
    done = invoke("dead-letter", "purge", *conn(dead), "--yes", "--error-type", "CardDeclined")
    assert "deleted 1" in done.stdout and total(dead) == 4
    assert "deleted 4" in invoke("dead-letter", "purge", *conn(dead), "--yes").stdout
    assert total(dead) == 0


def test_bad_option_values_are_rejected(dead):
    assert (
        invoke("dead-letter", "list", *conn(dead), "--header", "nonsense", ok=False).exit_code == 2
    )
    assert invoke("dead-letter", "list", *conn(dead), "--since", "2x", ok=False).exit_code == 2
