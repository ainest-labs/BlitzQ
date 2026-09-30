"""Aggregate raw benchmark runs into CSV/JSON summaries, charts and a Markdown report.

Usage::

    python -m benchmarks.report --input benchmarks/results/suite-<stamp>
    blitzq benchmark compare --input benchmarks/results/suite-<stamp>

Speedup is reported as BlitzQ median throughput / Celery median throughput,
and only when both systems completed every task in every repetition of the
same workload and profile (equal completion semantics). Otherwise the cell
says why no ratio is given.
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any

#: Workloads whose throughput is bounded by design (a fixed delay, a crash
#: recovery window), so a throughput ratio would be misleading.
NOT_THROUGHPUT = {
    "scheduled": "throughput bounded by the 2 s delay; compare schedule lateness",
    "worker-crash": "throughput dominated by recovery time; compare recovery and lost/dup",
    "redis-interruption": "compare recovery and duplicates",
}

METRICS = [
    "throughput",
    "enqueue_rate",
    "start_p50_ms",
    "start_p95_ms",
    "start_p99_ms",
    "e2e_p50_ms",
    "e2e_p99_ms",
    "exec_p50_ms",
    "worker_cpu_seconds",
    "worker_rss_peak_mb",
    "redis_cpu_seconds",
    "redis_mem_peak_mb",
    "redis_net_in_mb",
    "redis_net_out_mb",
    "broker_cmds_per_task",
    "recovery_seconds",
    "sched_lateness_p50_ms",
    "sched_lateness_p99_ms",
]


def load_runs(root: Path) -> tuple[list[dict[str, Any]], list[Path]]:
    rows: list[dict[str, Any]] = []
    dirs = sorted({p.parent for p in root.rglob("summary.jsonl")})
    for d in dirs:
        for line in (d / "summary.jsonl").read_text().splitlines():
            if line.strip():
                row = json.loads(line)
                row["run_dir"] = d.name
                rows.append(row)
    return rows, dirs


def _median(values: list[float]) -> float | None:
    vals = [v for v in values if v is not None]
    return statistics.median(vals) if vals else None


def aggregate(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        groups[(r["workload"], r["profile"], r["system"])].append(r)
    out = []
    for (workload, profile, system), rs in sorted(groups.items()):
        ok = [r for r in rs if "error" not in r]
        agg: dict[str, Any] = {
            "workload": workload,
            "profile": profile,
            "system": system,
            "runs": len(rs),
            "errors": len(rs) - len(ok),
            "tasks": ok[0]["tasks"] if ok else None,
            "settings": ok[0]["settings"] if ok else None,
            "all_completed": bool(ok) and all(r["not_completed"] == 0 for r in ok) and len(ok) == len(rs),
            "not_completed_max": max((r["not_completed"] for r in ok), default=None),
            "lost_total": sum((r["lost"] or 0) for r in ok),
            "outstanding_max": max((r["outstanding"] for r in ok), default=None),
            "duplicates_total": sum(r["duplicates"] for r in ok),
            "failed_attempts_median": _median([r["failed_attempts"] for r in ok]),
            "throughput_runs": [round(r["throughput"], 1) if r.get("throughput") else None for r in ok],
        }  # fmt: skip
        tps = [r["throughput"] for r in ok if r.get("throughput")]
        agg["throughput_min"] = min(tps) if tps else None
        agg["throughput_max"] = max(tps) if tps else None
        agg["throughput_cv_pct"] = (
            statistics.stdev(tps) / statistics.fmean(tps) * 100 if len(tps) > 1 else None
        )
        for m in METRICS:
            agg[m] = _median([r.get(m) for r in ok])
        for key in {k for r in ok for k in r if k.startswith("start_p") and "[" in k}:
            agg[key] = _median([r.get(key) for r in ok])
        if ok and agg["tasks"]:
            n = agg["tasks"]
            if agg["worker_cpu_seconds"] is not None:
                agg["worker_cpu_ms_per_task"] = agg["worker_cpu_seconds"] / n * 1000
            if agg["redis_cpu_seconds"] is not None:
                agg["redis_cpu_ms_per_task"] = agg["redis_cpu_seconds"] / n * 1000
        out.append(agg)
    return out


def speedups(aggs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """BlitzQ's throughput ratio against every other measured system."""
    by = {(a["workload"], a["profile"], a["system"]): a for a in aggs}
    others = sorted({a["system"] for a in aggs} - {"blitzq"})
    res = []
    for (wl, prof, sysname), a in sorted(by.items()):
        if sysname != "blitzq":
            continue
        for other in others:
            c = by.get((wl, prof, other))
            if c is None:
                continue
            row: dict[str, Any] = {"workload": wl, "profile": prof, "compared_to": other}
            if not (a["all_completed"] and c["all_completed"]):
                row["speedup"] = None
                row["note"] = "not comparable: " + ", ".join(
                    f"{s['system']} left {s['not_completed_max']} tasks incomplete"
                    for s in (a, c)
                    if not s["all_completed"]
                )
            elif a["throughput"] and c["throughput"]:
                row["speedup"] = a["throughput"] / c["throughput"]
                row["note"] = NOT_THROUGHPUT.get(wl, "")
            res.append(row)
    return res


def fmt(v: Any, spec: str = ".1f") -> str:
    if v is None:
        return "-"
    if isinstance(v, float):
        return format(v, spec)
    return str(v)


def charts(
    aggs: list[dict[str, Any]], rows: list[dict[str, Any]], dirs: list[Path], out: Path
) -> list[str]:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return []
    colors = {"blitzq": "#2f6fdd", "celery": "#3a9c5b", "huey": "#c9762e"}
    systems = sorted({a["system"] for a in aggs})
    n_systems = len(systems)
    bar_w = min(0.38, 0.8 / max(n_systems, 1))
    offsets = [(i - (n_systems - 1) / 2) * bar_w for i in range(n_systems)]
    made = []
    profiles = sorted({a["profile"] for a in aggs})
    for prof in profiles:
        sel = [a for a in aggs if a["profile"] == prof and a["throughput"]]
        workloads = sorted({a["workload"] for a in sel})
        if not workloads:
            continue
        # Throughput
        fig, ax = plt.subplots(figsize=(max(6, 1.3 * len(workloads)), 4))
        for i, system in enumerate(systems):
            xs, ys, lo, hi = [], [], [], []
            for j, wl in enumerate(workloads):
                a = next((x for x in sel if x["workload"] == wl and x["system"] == system), None)
                if a is None:
                    continue
                xs.append(j + offsets[i])
                ys.append(a["throughput"])
                lo.append(a["throughput"] - (a["throughput_min"] or a["throughput"]))
                hi.append((a["throughput_max"] or a["throughput"]) - a["throughput"])
            ax.bar(
                xs,
                ys,
                bar_w,
                yerr=[lo, hi],
                capsize=3,
                label=system,
                color=colors.get(system, "#888"),
            )
        ax.set_xticks(range(len(workloads)), workloads, rotation=30, ha="right")
        ax.set_ylabel("completed tasks / s (median, min-max)")
        ax.set_yscale("log")
        ax.set_title(f"Throughput - profile {prof}")
        ax.legend()
        ax.grid(axis="y", alpha=0.3)
        fig.tight_layout()
        name = f"throughput_{prof}.png"
        fig.savefig(out / name, dpi=110)
        plt.close(fig)
        made.append(name)

        # Latency percentiles
        lat = [a for a in sel if a.get("start_p50_ms") is not None]
        wls = sorted({a["workload"] for a in lat})
        if wls:
            fig, axes = plt.subplots(1, 3, figsize=(14, 4), sharey=True)
            for ax, p in zip(axes, ("50", "95", "99"), strict=True):
                for i, system in enumerate(systems):
                    xs, ys = [], []
                    for j, wl in enumerate(wls):
                        a = next(
                            (x for x in lat if x["workload"] == wl and x["system"] == system), None
                        )
                        if a and a.get(f"start_p{p}_ms") is not None:
                            xs.append(j + offsets[i])
                            ys.append(max(a[f"start_p{p}_ms"], 0.01))
                    ax.bar(xs, ys, bar_w, label=system, color=colors.get(system, "#888"))
                ax.set_xticks(range(len(wls)), wls, rotation=35, ha="right", fontsize=8)
                ax.set_yscale("log")
                ax.set_title(f"enqueue-to-start p{p}")
                ax.grid(axis="y", alpha=0.3)
            axes[0].set_ylabel("ms (median of runs, log)")
            axes[0].legend()
            fig.suptitle(f"Queue latency - profile {prof}")
            fig.tight_layout()
            name = f"latency_{prof}.png"
            fig.savefig(out / name, dpi=110)
            plt.close(fig)
            made.append(name)

        # Resource usage
        res = [a for a in sel if a.get("worker_cpu_ms_per_task") is not None]
        wls = sorted({a["workload"] for a in res})
        if wls:
            fig, axes = plt.subplots(1, 3, figsize=(14, 4))
            for ax, (key, label) in zip(
                axes,
                (
                    ("worker_cpu_ms_per_task", "worker CPU ms / task"),
                    ("redis_cpu_ms_per_task", "Redis CPU ms / task"),
                    ("worker_rss_peak_mb", "worker RSS peak (MB)"),
                ),
                strict=True,
            ):
                for i, system in enumerate(systems):
                    xs, ys = [], []
                    for j, wl in enumerate(wls):
                        a = next(
                            (x for x in res if x["workload"] == wl and x["system"] == system), None
                        )
                        if a and a.get(key) is not None:
                            xs.append(j + offsets[i])
                            ys.append(a[key])
                    ax.bar(xs, ys, bar_w, label=system, color=colors.get(system, "#888"))
                ax.set_xticks(range(len(wls)), wls, rotation=35, ha="right", fontsize=8)
                ax.set_title(label)
                ax.grid(axis="y", alpha=0.3)
            axes[0].legend()
            fig.suptitle(f"Resource usage - profile {prof}")
            fig.tight_layout()
            name = f"resources_{prof}.png"
            fig.savefig(out / name, dpi=110)
            plt.close(fig)
            made.append(name)

    # Backlog over time (repetition 0) for selected workloads
    for wl in ("burst-publish", "worker-crash", "noop", "noop-drain"):
        series = {}
        for d in dirs:
            for system in systems:
                for prof in profiles:
                    p = d / "raw" / f"{wl}-{prof}-{system}-r0.timeseries.csv"
                    if p.exists():
                        series[(prof, system)] = list(csv.DictReader(p.open()))
        if not series:
            continue
        fig, ax = plt.subplots(figsize=(8, 4))
        for (prof, system), pts in sorted(series.items()):
            ax.plot(
                [float(x["t"]) for x in pts],
                [float(x["backlog"]) for x in pts],
                label=f"{system} {prof}",
                color=colors.get(system, "#888"),
                linestyle="-" if prof.startswith("A") else "--",
            )
        ax.set_xlabel("seconds since measurement start")
        ax.set_ylabel("messages outstanding in broker")
        ax.set_title(f"Backlog over time - {wl} (repetition 0)")
        ax.legend(fontsize=8)
        ax.grid(alpha=0.3)
        fig.tight_layout()
        name = f"backlog_{wl}.png"
        fig.savefig(out / name, dpi=110)
        plt.close(fig)
        made.append(name)
    return made


def build_report(input_dir: str, output: str | None = None) -> Path:
    root = Path(input_dir)
    rows, dirs = load_runs(root)
    if not rows:
        raise SystemExit(f"no summary.jsonl found under {root}")
    out = Path(output).parent if output else root
    out.mkdir(parents=True, exist_ok=True)
    aggs = aggregate(rows)
    sp = speedups(aggs)

    keys = sorted({k for r in rows for k in r})
    with open(out / "runs.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(rows)
    akeys = sorted({k for a in aggs for k in a})
    with open(out / "aggregate.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=akeys)
        w.writeheader()
        w.writerows(aggs)
    envs = {}
    for d in dirs:
        if (d / "environment.json").exists():
            envs[d.name] = json.loads((d / "environment.json").read_text())
    (out / "summary.json").write_text(
        json.dumps({"aggregates": aggs, "speedups": sp, "environment": envs}, indent=2)
    )
    images = charts(aggs, rows, dirs, out)

    md: list[str] = ["# BlitzQ vs Celery - benchmark results", ""]
    md.append("Generated by `benchmarks/report.py` from raw run data; no number in this file is "
              "entered by hand.")  # fmt: skip
    md.append("")
    for name, env in envs.items():
        md += [
            f"## Environment ({name})",
            "",
            f"- Python: `{env['python'].split()[0]}` on `{env['platform']}`",
            f"- CPUs visible: {env['cpu_count_logical']} logical; memory {env['memory_gb']} GB",
            f"- Redis: {env['redis_version']}",
            "- Packages: " + ", ".join(f"{k} {v}" for k, v in env["packages"].items() if v),
            "",
        ]
        cmd = (root / name / "command.txt") if (root / name).exists() else root / "command.txt"
        if cmd.exists():
            md += ["Command:", "", "```", cmd.read_text().strip(), "```", ""]
    md += ["## Throughput speedup (BlitzQ / other system, median completed tasks per second)", ""]
    md += [
        "| workload | profile | vs | BlitzQ tasks/s | other tasks/s | speedup | note |",
        "|---|---|---|---:|---:|---:|---|",
    ]
    by = {(a["workload"], a["profile"], a["system"]): a for a in aggs}
    for s in sp:
        b = by[(s["workload"], s["profile"], "blitzq")]
        c = by[(s["workload"], s["profile"], s["compared_to"])]
        md.append(
            f"| {s['workload']} | {s['profile']} | {s['compared_to']} | "
            f"{fmt(b['throughput'], '.0f')} | {fmt(c['throughput'], '.0f')} | "
            f"{fmt(s['speedup'], '.2f') + 'x' if s['speedup'] else '-'} | {s['note']} |"
        )
    md += ["", "## Detailed results (medians across repetitions)", ""]
    md += [
        "| workload | profile | system | runs | tasks/s (min-max) | CV % | enqueue/s | "
        "start p50/p95/p99 ms | e2e p99 ms | not completed (max) | lost | dup | "
        "worker CPU ms/task | Redis CPU ms/task | Redis cmds/task | worker RSS MB |",
        "|---|---|---|---:|---|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for a in aggs:
        md.append(
            f"| {a['workload']} | {a['profile']} | {a['system']} | {a['runs']}"
            f"{' (' + str(a['errors']) + ' err)' if a['errors'] else ''} | "
            f"{fmt(a['throughput'], '.0f')} ({fmt(a['throughput_min'], '.0f')}-"
            f"{fmt(a['throughput_max'], '.0f')}) | {fmt(a['throughput_cv_pct'])} | "
            f"{fmt(a['enqueue_rate'], '.0f')} | {fmt(a['start_p50_ms'])}/{fmt(a['start_p95_ms'])}/"
            f"{fmt(a['start_p99_ms'])} | {fmt(a['e2e_p99_ms'])} | {fmt(a['not_completed_max'])} | "
            f"{a['lost_total']} | {a['duplicates_total']} | {fmt(a.get('worker_cpu_ms_per_task'), '.3f')} | "
            f"{fmt(a.get('redis_cpu_ms_per_task'), '.3f')} | {fmt(a['broker_cmds_per_task'], '.2f')} | "
            f"{fmt(a['worker_rss_peak_mb'], '.0f')} |"
        )  # fmt: skip
    special = [a for a in aggs if a.get("recovery_seconds") is not None
               or a.get("sched_lateness_p50_ms") is not None
               or any("[" in k for k in a)]  # fmt: skip
    if special:
        md += ["", "## Workload-specific metrics", ""]
        for a in special:
            extras = []
            if a.get("recovery_seconds") is not None:
                extras.append(
                    f"recovery (disruption -> last completion) {a['recovery_seconds']:.1f}s"
                )
            if a.get("sched_lateness_p50_ms") is not None:
                extras.append(f"schedule lateness p50 {a['sched_lateness_p50_ms']:.1f} ms, "
                              f"p99 {a['sched_lateness_p99_ms']:.1f} ms")  # fmt: skip
            qs = sorted(k for k in a if k.startswith("start_p") and "[" in k)
            if qs:
                extras.append(", ".join(f"{k.replace('start_', '')}={fmt(a[k])}ms" for k in qs))
            if a["failed_attempts_median"]:
                extras.append(f"failed attempts (median) {a['failed_attempts_median']:.0f}")
            md.append(
                f"- **{a['workload']} / {a['profile']} / {a['system']}**: " + "; ".join(extras)
            )
    md += ["", "## Individual runs (throughput, tasks/s)", ""]
    for a in aggs:
        md.append(f"- {a['workload']} / {a['profile']} / {a['system']}: {a['throughput_runs']}")
    md += ["", "## Settings per system and profile", ""]
    seen = set()
    for a in aggs:
        key = (a["profile"], a["system"], a["settings"])
        if key in seen:
            continue
        seen.add(key)
        md.append(f"- {a['workload']} / {a['profile']} / {a['system']}: `{a['settings']}`")
    if images:
        md += ["", "## Charts", ""]
        md += [f"![{i}]({i})" for i in images]
    md.append("")
    path = Path(output) if output else out / "REPORT.md"
    path.write_text("\n".join(md))
    return path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", default=None)
    args = ap.parse_args()
    print(build_report(args.input, args.output))


if __name__ == "__main__":
    main()
