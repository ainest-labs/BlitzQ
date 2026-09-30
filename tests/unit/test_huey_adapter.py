"""Huey adapter/report plumbing that doesn't need a running consumer or Redis."""

import pytest

pytest.importorskip("huey")
pytest.importorskip("yaml")
pytest.importorskip("psutil")

from benchmarks.adapters.huey import HueyAdapter
from benchmarks.workloads import Workload


def test_worker_commands_one_consumer_per_queue():
    w = Workload(name="mq", kind="multiqueue", queues={"bulk": 0.8, "normal": 0.2})
    a = HueyAdapter({"processes": 2, "worker_type": "greenlet"}, w, "redis://x/0")
    cmds = a.worker_commands()
    assert len(cmds) == 4  # 2 queues x 2 processes each
    modules = {c[3] for c in cmds}
    assert modules == {"benchmarks.apps.huey_app.huey_bulk", "benchmarks.apps.huey_app.huey_normal"}
    assert all("-k" in c and "greenlet" in c for c in cmds)


def test_worker_commands_default_queue_uses_app_module():
    w = Workload(name="noop", kind="noop")
    a = HueyAdapter({}, w, "redis://x/0")
    [cmd] = a.worker_commands()
    assert cmd[3] == "benchmarks.apps.huey_app.app"


def test_env_sets_worker_type_for_monkeypatching():
    w = Workload(name="io", kind="io")
    a = HueyAdapter({"worker_type": "greenlet"}, w, "redis://x/0")
    assert a.env()["BENCH_HUEY_WORKER_TYPE"] == "greenlet"


def test_backlog_sums_queue_and_schedule_keys():
    class FakeRedis:
        def __init__(self, values):
            self.values = values

        def llen(self, key):
            return self.values.get(key, 0)

        def zcard(self, key):
            return self.values.get(key, 0)

    w = Workload(name="mq", kind="multiqueue", queues={"bulk": 0.8, "normal": 0.2})
    a = HueyAdapter({}, w, "redis://x/0")
    r = FakeRedis({"huey.redis.bulk": 3, "huey.redis.normal": 1, "huey.schedule.bulk": 2})
    assert a.backlog(r) == 6  # type: ignore[arg-type]
