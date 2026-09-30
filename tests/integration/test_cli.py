"""CLI commands against a real Redis (sync tests: the CLI runs its own event loop)."""

import asyncio
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest
import redis
from typer.testing import CliRunner

from blitzq import Queue
from blitzq.cli import app as cli
from conftest import REDIS_URL, running, unique_ns

pytestmark = pytest.mark.redis
HERE = Path(__file__).parent
runner = CliRunner()


@pytest.fixture
def ns():
    ns = unique_ns()
    yield ns
    r = redis.Redis.from_url(REDIS_URL)
    for key in r.scan_iter(f"{ns}:*"):
        r.delete(key)


def invoke(*args: str):
    res = runner.invoke(cli, list(args), catch_exceptions=False)
    return res


def conn(ns: str) -> list[str]:
    return ["--redis-url", REDIS_URL, "--namespace", ns, "--mode", "fast"]


def run_worker_briefly(app: Queue, seconds: float = 0.5) -> None:
    async def go():
        async with running(app):
            await asyncio.sleep(seconds)
        await app.close()

    asyncio.run(go())


def test_task_lifecycle_commands(ns):
    app = Queue(redis_url=REDIS_URL, namespace=ns, mode="fast")

    @app.task(name="cli.fail")
    async def fail():
        raise ValueError("always fails")

    @app.task(name="cli.echo")
    async def echo(x):
        return x

    ok = echo.enqueue_sync(7)
    bad = fail.enqueue_sync()
    later = echo.options(delay=60).enqueue_sync(1)
    run_worker_briefly(app)

    res = invoke("task", "inspect", ok.id, *conn(ns))
    assert res.exit_code == 0 and "succeeded" in res.stdout and "result:      7" in res.stdout
    res = invoke("task", "inspect", ok.id, *conn(ns), "--json")
    assert json.loads(res.stdout)["result"] == 7

    res = invoke("dead-letter", "list", *conn(ns))
    assert "1 dead-lettered" in res.stdout and bad.id in res.stdout and "ValueError" in res.stdout
    res = invoke("dead-letter", "list", *conn(ns), "--json")
    assert json.loads(res.stdout)["items"][0]["id"] == bad.id

    res = invoke("task", "retry", bad.id, *conn(ns))
    assert res.exit_code == 0 and "re-enqueued" in res.stdout
    assert invoke("task", "retry", bad.id, *conn(ns)).exit_code == 1

    res = invoke("task", "cancel", later.id, *conn(ns))
    assert "cancelled scheduled task" in res.stdout
    res = invoke("task", "inspect", later.id, *conn(ns))
    assert "cancelled" in res.stdout

    assert invoke("task", "inspect", "nope", *conn(ns)).exit_code == 1
    app.close_sync()


def test_queue_stats_purge_and_dlq_purge(ns):
    app = Queue(redis_url=REDIS_URL, namespace=ns, mode="fast")

    @app.task(name="cli.echo")
    async def echo(x):
        return x

    echo.enqueue_many_sync((i,) for i in range(5))
    res = invoke("queue", "stats", *conn(ns), "--json")
    data = json.loads(res.stdout)
    assert {"name": "default", "waiting": 5, "in_progress": None} in data["queues"]
    res = invoke("queue", "stats", *conn(ns))
    assert "default" in res.stdout and "5" in res.stdout

    assert invoke("queue", "purge", "default", *conn(ns)).exit_code == 2  # needs --yes
    res = invoke("queue", "purge", "default", "--yes", *conn(ns))
    assert "purged 5" in res.stdout
    assert invoke("dead-letter", "purge", "--yes", *conn(ns)).exit_code == 0
    app.close_sync()


def test_worker_and_scheduler_subprocesses(ns):
    env = {
        **os.environ,
        "BQ_URL": REDIS_URL,
        "BQ_NS": ns,
        "PYTHONPATH": os.pathsep.join([str(HERE), os.environ.get("PYTHONPATH", "")]),
    }
    sched = subprocess.Popen(
        [sys.executable, "-m", "blitzq", "scheduler", "cli_app:app", "--poll-interval", "0.1"],
        cwd=HERE,
        env=env,
        stderr=subprocess.DEVNULL,
    )
    worker = subprocess.Popen(
        [sys.executable, "-m", "blitzq", "worker", "cli_app:app", "-Q", "default", "-c", "5",
         "--log-format", "json"],
        cwd=HERE,
        env=env,
        stderr=subprocess.PIPE,
    )  # fmt: skip
    try:
        time.sleep(4)
        r = redis.Redis.from_url(REDIS_URL)
        last = r.hget(f"{ns}:periodic", "cli.tick")
        assert last is not None
        res = invoke("queue", "stats", *conn(ns), "--json")
        workers = json.loads(res.stdout)["workers"]
        assert len(workers) == 1 and workers[0]["processed"] >= 3
    finally:
        sched.terminate()
        worker.terminate()
        sched.wait(10)
        _, err = worker.communicate(timeout=10)
    first = err.decode().splitlines()[0]
    assert json.loads(first)["msg"] == "worker started"


def test_load_app_errors():
    import typer

    from blitzq.cli import load_app

    with pytest.raises(typer.BadParameter):
        load_app("no_such_module_xyz:app")
    with pytest.raises(typer.BadParameter):
        load_app("json:dumps")
