"""The benchmark's accounting must count completions, losses and duplicates exactly."""

import json

import pytest

pytest.importorskip("yaml")
pytest.importorskip("psutil")

from benchmarks import report
from benchmarks.produce import items_for
from benchmarks.runner import Records, percentile
from benchmarks.workloads import Workload


def line(uid, outcome="ok", attempt=1, te=0.0, ts=1.0, tend=1.5, q="default", pid=1):
    return f"{uid},{outcome},{attempt},{te!r},{ts!r},{tend!r},{q},{pid}".encode()


def test_percentile_interpolates_like_numpy_linear():
    vals = [float(v) for v in range(1, 101)]
    assert percentile(vals, 50) == pytest.approx(50.5)
    assert percentile(vals, 99) == pytest.approx(99.01)
    assert percentile([3.0], 99) == 3.0
    assert percentile([], 50) is None


def test_records_first_success_per_uid_and_duplicates():
    raw = [
        line(1, ts=1.0),
        line(2, outcome="fail", ts=1.0),
        line(2, attempt=2, ts=2.0),
        line(1, ts=5.0),  # duplicate execution of uid 1
        line(3, ts=1.2),
    ]
    recs = Records(raw)
    first = recs.ok_first()
    assert set(first) == {1, 2, 3}
    assert first[1][4] == 1.0  # the first successful execution is kept
    ok_rows = [r for r in recs.rows if r[1] == "ok"]
    assert len(ok_rows) - len(first) == 1  # exactly one duplicate
    assert len([r for r in recs.rows if r[1] == "fail"]) == 1


def test_items_distribution_and_unique_ids():
    w = Workload(name="m", kind="multiqueue", tasks=10_000, queues={"a": 0.8, "b": 0.2})
    items = items_for(w, 0, 10_000, seed=1)
    assert len({uid for uid, _, _ in items}) == 10_000
    share_a = sum(1 for _, q, _ in items if q == "a") / 10_000
    assert 0.77 < share_a < 0.83
    sched = items_for(Workload(name="s", kind="scheduled", delay_s=2.0), 5, 3, seed=1)
    assert [(u, d) for u, _, d in sched] == [(5, 2.0), (6, 2.0), (7, 2.0)]


def _row(system, tput, not_completed=0, rep=0, **kw):
    base = {
        "workload": "w", "profile": "A", "system": system, "rep": rep, "tasks": 100,
        "settings": "{}", "not_completed": not_completed, "lost": not_completed,
        "outstanding": 0, "duplicates": 0, "failed_attempts": 0, "throughput": tput,
    }  # fmt: skip
    base.update(kw)
    return base


def test_speedup_uses_medians_and_requires_complete_runs():
    rows = [_row("blitzq", t, rep=i) for i, t in enumerate([100, 300, 200])]
    rows += [_row("celery", t, rep=i) for i, t in enumerate([50, 100, 1000])]
    aggs = report.aggregate(rows)
    sp = report.speedups(aggs)
    assert sp[0]["speedup"] == pytest.approx(200 / 100)

    rows.append(_row("celery", 100, not_completed=3, rep=3))
    sp = report.speedups(report.aggregate(rows))
    assert sp[0]["speedup"] is None and "celery left 3" in sp[0]["note"]


def test_report_end_to_end(tmp_path):
    run = tmp_path / "suite-x"
    (run / "raw").mkdir(parents=True)
    rows = [_row("blitzq", 1000.0, rep=r, start_p50_ms=1.0) for r in range(2)]
    rows += [_row("celery", 500.0, rep=r, start_p50_ms=5.0) for r in range(2)]
    rows.append({"workload": "w", "profile": "A", "system": "celery", "rep": 9, "error": "boom"})
    (run / "summary.jsonl").write_text("\n".join(json.dumps(r) for r in rows))
    path = report.build_report(str(tmp_path))
    text = path.read_text()
    assert "no number in this file is entered by hand" in text
    assert "not comparable" in text  # one Celery run errored -> no ratio claimed
    data = json.loads((tmp_path / "summary.json").read_text())
    celery = next(a for a in data["aggregates"] if a["system"] == "celery")
    assert celery["errors"] == 1 and celery["runs"] == 3
