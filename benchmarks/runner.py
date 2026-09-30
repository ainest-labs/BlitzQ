"""Benchmark runner: executes workload configs against BlitzQ and Celery.

Usage::

    python -m benchmarks.runner --config benchmarks/configs/suite.yaml
    blitzq benchmark run --config benchmarks/configs/noop.yaml

For every (workload, profile, repetition, system) the runner:

1. flushes the broker Redis and the stats Redis (isolated namespaces per run),
2. starts the system's worker processes,
3. runs a warm-up batch and waits for it to complete, then discards its records,
4. starts resource samplers (worker CPU/RSS via psutil, broker backlog,
   Redis memory) and snapshots Redis ``INFO`` counters,
5. runs producer subprocesses (enqueue timestamps are embedded in each task),
6. optionally disrupts the run (kills a worker / drops Redis connections),
7. waits until every task has a successful execution record, or the run is
   quiescent (nothing outstanding in the broker and no new records), or the
   timeout expires - outstanding work is reported, never hidden,
8. stops the workers and writes raw per-task CSV, time series and a summary.

Systems alternate order between repetitions to spread drift (thermal, page
cache) evenly.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import platform
import statistics
import subprocess
import sys
import threading
import time
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psutil
import redis
import yaml

from .adapters import get_adapter
from .adapters.base import Adapter
from .recorder import RECORDS_KEY
from .workloads import Workload

ROOT = Path(__file__).resolve().parent.parent
BROKER_URL = os.environ.get("BLITZQ_BENCH_BROKER_URL", "redis://localhost:6379/0")
STATS_URL = os.environ.get("BLITZQ_BENCH_STATS_URL", "redis://localhost:6380/0")
WARMUP_UID = 1_000_000_000


def percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    s = sorted(values)
    k = (len(s) - 1) * p / 100
    lo = int(k)
    hi = min(lo + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


class Records:
    def __init__(self, raw: list[bytes]) -> None:
        self.rows: list[tuple[int, str, int, float, float, float, str, int]] = []
        for line in raw:
            uid, outcome, attempt, te, ts, tend, q, pid = line.decode().split(",")
            self.rows.append(
                (int(uid), outcome, int(attempt), float(te), float(ts), float(tend), q, int(pid))
            )

    def ok_first(self) -> dict[int, tuple[Any, ...]]:
        """First successful execution per uid."""
        out: dict[int, tuple[Any, ...]] = {}
        for r in self.rows:
            if r[1] == "ok" and r[0] not in out:
                out[r[0]] = r
        return out


class Sampler(threading.Thread):
    def __init__(
        self, adapter: Adapter, broker: redis.Redis, procs: list[subprocess.Popen]
    ) -> None:
        super().__init__(daemon=True)
        self.adapter = adapter
        self.broker = broker
        self.procs = procs
        self.stop_flag = threading.Event()
        self.series: list[dict[str, float]] = []
        self.cpu_last: dict[int, float] = {}
        self.cpu_base: dict[int, float] = {}
        self.t0 = time.perf_counter()
        self._ps: dict[int, psutil.Process] = {}

    def _tree(self) -> list[psutil.Process]:
        out = []
        for p in list(self.procs):
            try:
                root = self._ps.setdefault(p.pid, psutil.Process(p.pid))
                out.append(root)
                out.extend(root.children(recursive=True))
            except psutil.Error:
                continue
        return out

    def run(self) -> None:
        first = True
        while not self.stop_flag.is_set():
            cpu_pct = rss = 0.0
            for proc in self._tree():
                try:
                    pid = proc.pid
                    proc = self._ps.setdefault(pid, proc)
                    t = proc.cpu_times()
                    total = t.user + t.system
                    if pid not in self.cpu_base:
                        self.cpu_base[pid] = total if first else 0.0
                    self.cpu_last[pid] = total
                    cpu_pct += proc.cpu_percent(None)
                    rss += proc.memory_info().rss
                except psutil.Error:
                    continue
            first = False
            try:
                backlog = self.adapter.backlog(self.broker)
                mem = int(self.broker.info("memory")["used_memory"])
            except redis.RedisError:
                backlog, mem = -1, -1
            self.series.append(
                {
                    "t": time.perf_counter() - self.t0,
                    "backlog": backlog,
                    "worker_cpu_pct": cpu_pct,
                    "worker_rss_mb": rss / 1e6,
                    "redis_mem_mb": mem / 1e6,
                }
            )
            self.stop_flag.wait(0.25)

    def worker_cpu_seconds(self) -> float:
        return sum(self.cpu_last[p] - self.cpu_base.get(p, 0.0) for p in self.cpu_last)


def redis_counters(r: redis.Redis) -> dict[str, float]:
    info = r.info()
    return {
        "cpu": float(info["used_cpu_user"]) + float(info["used_cpu_sys"]),
        "cmds": float(info["total_commands_processed"]),
        "net_in": float(info["total_net_input_bytes"]),
        "net_out": float(info["total_net_output_bytes"]),
    }


def start_workers(
    adapter: Adapter, env: dict[str, str], logdir: Path, tag: str
) -> list[subprocess.Popen]:
    procs = []
    for i, cmd in enumerate(adapter.worker_commands()):
        log = open(logdir / f"{tag}-worker{i}.log", "ab")  # noqa: SIM115 - closed with process
        procs.append(subprocess.Popen(cmd, cwd=ROOT, env=env, stdout=log, stderr=log))
    return procs


def stop_workers(procs: list[subprocess.Popen]) -> None:
    for p in procs:
        if p.poll() is None:
            p.terminate()
    deadline = time.monotonic() + 15
    for p in procs:
        try:
            p.wait(max(0.1, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            kill_tree(p)


def kill_tree(p: subprocess.Popen) -> None:
    try:
        root = psutil.Process(p.pid)
        for c in root.children(recursive=True):
            c.kill()
        root.kill()
    except psutil.Error:
        pass
    try:
        p.wait(10)
    except subprocess.TimeoutExpired:
        pass


def run_producers(
    system: str, settings: dict[str, Any], w: Workload, env: dict[str, str], start: int,
    count: int, n_producers: int, seed: int,
) -> list[dict[str, Any]]:  # fmt: skip
    share = count // n_producers
    procs = []
    for i in range(n_producers):
        c = share if i < n_producers - 1 else count - share * (n_producers - 1)
        spec = {
            "system": system,
            "settings": settings,
            "workload": asdict(w),
            "broker_url": BROKER_URL,
            "start": start + i * share,
            "count": c,
            "seed": seed + i,
        }
        procs.append(
            subprocess.Popen(
                [sys.executable, "-m", "benchmarks.produce", json.dumps(spec)],
                cwd=ROOT,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        )
    out = []
    for p in procs:
        stdout, stderr = p.communicate(timeout=w.timeout)
        if p.returncode != 0:
            raise RuntimeError(f"producer failed: {stderr.decode()[-2000:]}")
        out.append(json.loads(stdout.decode().strip().splitlines()[-1]))
    return out


def wait_complete(
    stats: redis.Redis, adapter: Adapter, broker: redis.Redis, w: Workload, n: int,
    expected_min: int, deadline: float, on_progress: Any = None,
) -> Records:  # fmt: skip
    last_len, last_change = -1, time.monotonic()
    while True:
        length = int(stats.llen(RECORDS_KEY))  # type: ignore[arg-type]
        now = time.monotonic()
        if length != last_len:
            last_len, last_change = length, now
        if on_progress is not None:
            on_progress(length)
        if length >= expected_min:
            recs = Records(stats.lrange(RECORDS_KEY, 0, -1))  # type: ignore[arg-type]
            if len(recs.ok_first()) >= n:
                time.sleep(0.3)  # let duplicate executions (if any) land
                return Records(stats.lrange(RECORDS_KEY, 0, -1))  # type: ignore[arg-type]
        quiet = now - last_change > 5
        if quiet:
            try:
                if adapter.backlog(broker) == 0:
                    return Records(stats.lrange(RECORDS_KEY, 0, -1))  # type: ignore[arg-type]
            except redis.RedisError:
                pass
        if now > deadline:
            return Records(stats.lrange(RECORDS_KEY, 0, -1))  # type: ignore[arg-type]
        time.sleep(0.05)


def run_once(
    system: str, settings: dict[str, Any], w: Workload, profile: str, rep: int, out: Path
) -> dict[str, Any]:
    broker = redis.Redis.from_url(BROKER_URL)
    stats = redis.Redis.from_url(STATS_URL)
    broker.flushdb()
    stats.delete(RECORDS_KEY)
    adapter = get_adapter(system, settings, w, BROKER_URL)
    env = {
        **os.environ,
        **adapter.env(),
        "BLITZQ_BENCH_STATS_URL": STATS_URL,
        "PYTHONPATH": os.pathsep.join(
            [str(ROOT), str(ROOT / "src"), os.environ.get("PYTHONPATH", "")]
        ),
    }
    tag = f"{w.name}-{profile}-{system}-r{rep}"
    logdir = out / "logs"
    logdir.mkdir(parents=True, exist_ok=True)
    procs = start_workers(adapter, env, logdir, tag)
    result: dict[str, Any] = {
        "workload": w.name, "kind": w.kind, "profile": profile, "system": system, "rep": rep,
        "tasks": w.tasks, "settings": json.dumps(adapter.describe(), sort_keys=True),
    }  # fmt: skip
    try:
        # -- warm-up ----------------------------------------------------------------
        warm = Workload(**{**asdict(w), "kind": "noop", "tasks": w.warmup, "payload_bytes": 0})
        warm_ok = False
        if w.warmup:
            run_producers(system, settings, warm, env, WARMUP_UID, w.warmup, 1, 7)
            recs = wait_complete(stats, adapter, broker, warm, w.warmup, w.warmup,
                                 time.monotonic() + 120)  # fmt: skip
            warm_ok = len(recs.ok_first()) >= w.warmup
            if not warm_ok:
                raise RuntimeError(f"warm-up incomplete for {tag}; see {logdir}")
        stats.delete(RECORDS_KEY)
        time.sleep(0.5)

        # -- measured run -----------------------------------------------------------
        before = redis_counters(broker)
        sampler = Sampler(adapter, broker, procs)
        sampler.start()
        disruption: dict[str, float] = {}
        producers_done = threading.Event()
        # Profiles may set the producer-process count (applies to both systems).
        n_prod = int(settings.get("producers", w.producers))
        n = w.tasks
        expected_min = n + (n // w.fail_every if w.kind == "retry" else 0)

        def on_progress(length: int) -> None:
            if w.kind not in ("crash", "interrupt") or disruption:
                return
            # Connection drops are applied after publishing so they measure
            # consumer-side recovery, not producer error handling.
            if w.kind == "interrupt" and not producers_done.is_set():
                return
            if length >= w.disrupt_at * n:
                disruption["t"] = time.perf_counter()
                if w.kind == "crash":
                    kill_tree(procs[0])  # abrupt: SIGKILL of the whole process tree
                    replacement = start_workers(adapter, env, logdir, tag + "-replacement")[0]
                    procs[0] = replacement
                else:
                    admin = redis.Redis.from_url(BROKER_URL)
                    disruption["killed_clients"] = float(
                        admin.client_kill_filter(_type="normal", skipme=True)
                    )

        waiter_result: dict[str, Records] = {}
        deadline = time.monotonic() + w.timeout

        def waiter() -> None:
            waiter_result["r"] = wait_complete(
                stats, adapter, broker, w, n, expected_min, deadline, on_progress
            )

        if w.preload:
            # Stop consumers, publish everything, then start fresh consumers.
            sampler.stop_flag.set()
            sampler.join()
            stop_workers(procs)
            prods = run_producers(system, settings, w, env, 0, n, n_prod, 1000 + rep)
            producers_done.set()
            procs[:] = start_workers(adapter, env, logdir, tag + "-drain")
            before = redis_counters(broker)
            sampler = Sampler(adapter, broker, procs)
            sampler.start()
            deadline = time.monotonic() + w.timeout
        wt = threading.Thread(target=waiter, daemon=True)
        wt.start()
        if not w.preload:
            prods = run_producers(system, settings, w, env, 0, n, n_prod, 1000 + rep)
            producers_done.set()
        wt.join()
        recs = waiter_result["r"]
        sampler.stop_flag.set()
        sampler.join()
        after = redis_counters(broker)
        outstanding = adapter.backlog(broker)
    finally:
        stop_workers(procs)

    # -- metrics ---------------------------------------------------------------------
    t_prod_start = min(p["t_start"] for p in prods)
    t_prod_end = max(p["t_end"] for p in prods)
    ok = recs.ok_first()
    ok_rows = [r for r in recs.rows if r[1] == "ok"]
    measured_ok = {u: r for u, r in ok.items() if u < WARMUP_UID}
    completed = len(measured_ok)
    t_last = max((r[5] for r in measured_ok.values()), default=t_prod_start)
    # Throughput window: from the first enqueue (or, for preloaded drain runs,
    # the first task start) to the last task end.
    t_begin = t_prod_start
    if w.preload and measured_ok:
        t_begin = min(r[4] for r in measured_ok.values())
    exec_t = [r[5] - r[4] for r in measured_ok.values()]
    if w.preload:
        # Queue latency is meaningless when tasks were deliberately held back.
        start_lat, e2e = [], []
    else:
        start_lat = [r[4] - r[3] for r in measured_ok.values()]
        e2e = [r[5] - r[3] for r in measured_ok.values()]
    elapsed = t_last - t_begin
    result.update(
        completed=completed,
        # Tasks with no successful execution. If `outstanding` is 0 they are
        # lost; otherwise some are still in the broker (e.g. awaiting redelivery).
        not_completed=n - completed,
        lost=(n - completed) if outstanding == 0 else None,
        duplicates=len([r for r in ok_rows if r[0] < WARMUP_UID]) - completed,
        executions=len([r for r in recs.rows if r[0] < WARMUP_UID]),
        failed_attempts=len([r for r in recs.rows if r[1] == "fail"]),
        outstanding=outstanding,
        enqueue_seconds=t_prod_end - t_prod_start,
        enqueue_rate=n / (t_prod_end - t_prod_start) if t_prod_end > t_prod_start else None,
        elapsed_seconds=elapsed,
        throughput=completed / elapsed if elapsed > 0 else None,
        worker_cpu_seconds=sampler.worker_cpu_seconds(),
        worker_cpu_pct_mean=statistics.fmean(s["worker_cpu_pct"] for s in sampler.series)
        if sampler.series else None,
        worker_rss_peak_mb=max((s["worker_rss_mb"] for s in sampler.series), default=None),
        redis_cpu_seconds=after["cpu"] - before["cpu"],
        redis_mem_peak_mb=max((s["redis_mem_mb"] for s in sampler.series), default=None),
        redis_net_in_mb=(after["net_in"] - before["net_in"]) / 1e6,
        redis_net_out_mb=(after["net_out"] - before["net_out"]) / 1e6,
        broker_cmds_per_task=(after["cmds"] - before["cmds"]) / n,
    )  # fmt: skip
    for name, values in (("start", start_lat), ("exec", exec_t), ("e2e", e2e)):
        for p in (50, 95, 99):
            v = percentile(values, p)
            result[f"{name}_p{p}_ms"] = v * 1000 if v is not None else None
        result[f"{name}_max_ms"] = max(values) * 1000 if values else None
    if w.kind == "scheduled":
        lateness = [r[4] - (r[3] + w.delay_s) for r in measured_ok.values()]
        for p in (50, 95, 99):
            v = percentile(lateness, p)
            result[f"sched_lateness_p{p}_ms"] = v * 1000 if v is not None else None
    if w.kind == "multiqueue":
        for q in w.queues:
            qlat = [r[4] - r[3] for r in measured_ok.values() if r[6] == q]
            for p in (50, 99):
                v = percentile(qlat, p)
                result[f"start_p{p}_ms[{q}]"] = v * 1000 if v is not None else None
    if disruption:
        result["disrupted_at_s"] = disruption["t"] - t_prod_start
        result["recovery_seconds"] = t_last - disruption["t"] if completed else None
        if "killed_clients" in disruption:
            result["killed_clients"] = disruption["killed_clients"]

    # -- raw output ------------------------------------------------------------------
    raw_dir = out / "raw"
    raw_dir.mkdir(exist_ok=True)
    with open(raw_dir / f"{tag}.csv", "w", newline="") as f:
        wr = csv.writer(f)
        wr.writerow(["uid", "outcome", "attempt", "t_enqueue", "t_start", "t_end", "queue", "pid"])
        wr.writerows(r for r in recs.rows if r[0] < WARMUP_UID)
    with open(raw_dir / f"{tag}.timeseries.csv", "w", newline="") as f:
        if sampler.series:
            wr2 = csv.DictWriter(f, fieldnames=list(sampler.series[0]))
            wr2.writeheader()
            wr2.writerows(sampler.series)
    return result


def environment() -> dict[str, Any]:
    import importlib.metadata as md

    broker = redis.Redis.from_url(BROKER_URL)
    server = broker.info("server")
    versions = {}
    for pkg in ("blitzq", "celery", "kombu", "huey", "redis", "msgspec", "gevent", "billiard"):
        try:
            versions[pkg] = md.version(pkg)
        except md.PackageNotFoundError:
            versions[pkg] = None
    if versions["blitzq"] is None:
        sys.path.insert(0, str(ROOT / "src"))
        from blitzq import __version__

        versions["blitzq"] = __version__ + " (source)"
    return {
        "timestamp": datetime.now(UTC).isoformat(),
        "python": sys.version,
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_count_logical": psutil.cpu_count(),
        "cpu_count_physical": psutil.cpu_count(logical=False),
        "memory_gb": round(psutil.virtual_memory().total / 1e9, 1),
        "cpu_affinity": len(psutil.Process().cpu_affinity() or []) if hasattr(psutil.Process(), "cpu_affinity") else None,
        "redis_version": server.get("redis_version"),
        "redis_mode": server.get("redis_mode"),
        "packages": versions,
        "broker_url": BROKER_URL,
        "stats_url": STATS_URL,
    }  # fmt: skip


def run_config(
    config: str,
    output: str = "benchmarks/results",
    systems: list[str] | None = None,
    repetitions: int | None = None,
    only: list[str] | None = None,
    scale: float = 1.0,
) -> Path:
    cfg = yaml.safe_load(Path(config).read_text())
    reps = repetitions or int(cfg.get("repetitions", 5))
    systems = systems or list(cfg.get("systems", ["blitzq", "celery"]))
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out = Path(output) / f"{cfg.get('name', Path(config).stem)}-{stamp}"
    out.mkdir(parents=True, exist_ok=True)
    (out / "config.yaml").write_text(Path(config).read_text())
    (out / "environment.json").write_text(json.dumps(environment(), indent=2))
    command = " ".join([Path(sys.executable).name, "-m", "benchmarks.runner", *sys.argv[1:]])
    (out / "command.txt").write_text(command + "\n")
    defaults = cfg.get("defaults", {})
    profiles = cfg["profiles"]
    summary_path = out / "summary.jsonl"
    print(f"results -> {out}", flush=True)
    for wd in cfg["workloads"]:
        wd = dict(wd)
        wl_profiles = wd.pop("profiles", list(profiles))
        overrides = wd.pop("overrides", {})
        if scale != 1.0:
            wd["tasks"] = max(100, int(wd.get("tasks", 10_000) * scale))
            wd["warmup"] = max(50, int(wd.get("warmup", 500) * scale))
        w = Workload.from_dict(wd)
        if only and w.name not in only:
            continue
        for profile in wl_profiles:
            for rep in range(reps):
                order = systems if rep % 2 == 0 else list(reversed(systems))
                for system in order:
                    if system not in profiles[profile]:
                        continue
                    settings = {
                        **defaults,
                        **profiles[profile].get(system, {}),
                        **overrides.get(profile, {}).get(system, {}),
                    }
                    t = time.monotonic()
                    try:
                        res = run_once(system, settings, w, profile, rep, out)
                    except Exception as exc:
                        res = {"workload": w.name, "profile": profile, "system": system,
                               "rep": rep, "error": repr(exc)}  # fmt: skip
                    with open(summary_path, "a") as f:
                        f.write(json.dumps(res) + "\n")
                    print(f"[{w.name}/{profile}/{system}/r{rep}] {_brief(res, w)} "
                          f"({time.monotonic() - t:.0f}s)", flush=True)  # fmt: skip
    return out


def _brief(res: dict[str, Any], w: Workload) -> str:
    if "error" in res:
        return f"error={res['error']}"

    def f(key: str, fmt: str) -> str:
        v = res.get(key)
        return "-" if v is None else format(v, fmt)

    return (
        f"done={res['completed']}/{w.tasks} tput={f('throughput', '.0f')}/s "
        f"enq={f('enqueue_rate', '.0f')}/s start_p50={f('start_p50_ms', '.1f')}ms "
        f"p99={f('start_p99_ms', '.1f')}ms not_completed={res['not_completed']} "
        f"outstanding={res['outstanding']} dup={res['duplicates']}"
    )


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--config", required=True)
    ap.add_argument("--output", default="benchmarks/results")
    ap.add_argument("--systems", default=None)
    ap.add_argument("--repetitions", type=int, default=None)
    ap.add_argument("--only", default=None, help="comma-separated workload names")
    ap.add_argument("--scale", type=float, default=1.0, help="multiply task counts (quick runs)")
    args = ap.parse_args()
    run_config(
        args.config,
        args.output,
        systems=args.systems.split(",") if args.systems else None,
        repetitions=args.repetitions,
        only=args.only.split(",") if args.only else None,
        scale=args.scale,
    )


if __name__ == "__main__":
    main()
