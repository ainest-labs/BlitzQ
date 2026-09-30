"""Command-line interface (``blitzq``)."""

from __future__ import annotations

import asyncio
import importlib
import json
import multiprocessing
import os
import signal
import sys
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Annotated, Any, Literal, TypedDict, TypeVar

import msgspec
import typer

from ._version import __version__
from .client import Queue
from .logs import configure_logging

T = TypeVar("T")

app = typer.Typer(
    name="blitzq",
    help="BlitzQ task queue: run workers and schedulers, inspect queues and tasks.",
    no_args_is_help=True,
    add_completion=False,
    pretty_exceptions_enable=False,
)
queue_app = typer.Typer(help="Inspect and manage queues.", no_args_is_help=True)
task_app = typer.Typer(help="Inspect, retry and cancel tasks.", no_args_is_help=True)
dlq_app = typer.Typer(help="Inspect and manage dead-lettered tasks.", no_args_is_help=True)
bench_app = typer.Typer(
    help="Run the BlitzQ vs Celery benchmark suite (requires a repository checkout).",
    no_args_is_help=True,
)
app.add_typer(queue_app, name="queue")
app.add_typer(task_app, name="task")
app.add_typer(dlq_app, name="dead-letter")
app.add_typer(bench_app, name="benchmark")


def load_app(path: str) -> Queue:
    """Import ``module:attribute`` (or ``module``, finding its single Queue)."""
    if os.getcwd() not in sys.path:
        sys.path.insert(0, os.getcwd())
    module_name, _, attr = path.partition(":")
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise typer.BadParameter(f"cannot import {module_name!r}: {exc}") from exc
    if attr:
        obj: Any = module
        for part in attr.split("."):
            obj = getattr(obj, part, None)
            if obj is None:
                raise typer.BadParameter(f"{module_name!r} has no attribute {attr!r}")
        if not isinstance(obj, Queue):
            raise typer.BadParameter(f"{path!r} is not a blitzq.Queue")
        return obj
    found = [v for v in vars(module).values() if isinstance(v, Queue)]
    if len(found) != 1:
        raise typer.BadParameter(f"specify the Queue attribute explicitly, e.g. {module_name}:app")
    return found[0]


AppOpt = Annotated[
    str | None,
    typer.Option("--app", "-A", envvar="BLITZQ_APP", help="Application as module:attribute."),
]
UrlOpt = Annotated[
    str | None, typer.Option("--redis-url", envvar="BLITZQ_REDIS_URL", help="Redis URL.")
]
ModeOpt = Annotated[
    str, typer.Option("--mode", envvar="BLITZQ_MODE", help="fast or reliable (without --app).")
]
NsOpt = Annotated[str, typer.Option("--namespace", envvar="BLITZQ_NAMESPACE")]
JsonOpt = Annotated[bool, typer.Option("--json", help="Machine-readable output.")]


def _client(app_path: str | None, url: str | None, mode: str, namespace: str) -> Queue:
    if app_path:
        return load_app(app_path)
    if mode not in ("fast", "reliable"):
        raise typer.BadParameter("--mode must be 'fast' or 'reliable'")
    m: Literal["fast", "reliable"] = "fast" if mode == "fast" else "reliable"
    return Queue(redis_url=url, mode=m, namespace=namespace)


def _run(q: Queue, fn: Callable[[], Awaitable[T]]) -> T:
    async def main() -> T:
        try:
            return await fn()
        finally:
            await q.close()

    return asyncio.run(main())


def _ts(value: float | None) -> str:
    if not value:
        return "-"
    return datetime.fromtimestamp(value, UTC).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "Z"


def _to_builtins(obj: Any) -> Any:
    return msgspec.to_builtins(obj, builtin_types=(bytes,), enc_hook=repr)


# -- worker / scheduler ------------------------------------------------------------
def _parse_queue_concurrency(values: list[str]) -> dict[str, int]:
    out: dict[str, int] = {}
    for value in values:
        for item in value.split(","):
            name, sep, n = item.partition("=")
            if not sep or not n.isdigit():
                raise typer.BadParameter(f"expected QUEUE=N, got {item!r}")
            out[name.strip()] = int(n)
    return out


class _WorkerKwargs(TypedDict):
    app_path: str
    queues: list[str] | None
    concurrency: int
    queue_concurrency: dict[str, int]
    batch_size: int | None
    threads: int | None
    processes: int | None
    shutdown_timeout: float
    promote: bool
    warn_cpu_bound: bool
    include: list[str]
    name: str | None
    metrics_port: int | None
    log_level: str
    log_format: str


def _run_one_worker(
    app_path: str,
    queues: list[str] | None,
    concurrency: int,
    queue_concurrency: dict[str, int],
    batch_size: int | None,
    threads: int | None,
    processes: int | None,
    shutdown_timeout: float,
    promote: bool,
    warn_cpu_bound: bool,
    include: list[str],
    name: str | None,
    metrics_port: int | None,
    log_level: str,
    log_format: str,
    stop_event: Any = None,
) -> None:
    """Build and run one worker. Also the entry point for each child process
    under ``--workers N > 1`` (must be a top-level function: multiprocessing
    on Windows pickles the target rather than forking).

    ``stop_event``, when given (pool mode only), is a multiprocessing.Event
    the parent sets on shutdown; a background thread turns that into
    ``worker.stop()``. OS signal forwarding isn't used for this because
    ``os.kill(pid, SIGTERM)`` doesn't deliver a catchable signal on
    Windows -- it hard-kills the process there, skipping graceful drain
    entirely. A single (non-pool) worker still shuts down on SIGINT/SIGTERM/
    SIGBREAK exactly as before, via run_worker's own handler.
    """
    from .metrics import Metrics, start_prometheus_exporter
    from .worker import Worker, run_worker

    configure_logging(log_level, log_format)
    q = load_app(app_path)
    for mod in include:
        importlib.import_module(mod)
    metrics = Metrics()
    if metrics_port:
        start_prometheus_exporter(metrics, metrics_port)
    w = Worker(
        q,
        queues=queues,
        concurrency=concurrency,
        queue_concurrency=queue_concurrency,
        batch_size=batch_size,
        threads=threads,
        processes=processes,
        shutdown_timeout=shutdown_timeout,
        promote=promote,
        warn_cpu_bound=warn_cpu_bound,
        name=name,
        metrics=metrics,
    )
    if stop_event is not None:
        import threading

        def watch() -> None:
            stop_event.wait()
            w.stop()

        threading.Thread(target=watch, daemon=True).start()
    run_worker(w)


def _run_worker_pool(
    num_workers: int, base_metrics_port: int | None, kwargs: _WorkerKwargs
) -> None:
    """Spawn ``num_workers`` worker processes and supervise them like a
    prefork pool: signal every child to drain and stop on shutdown, and
    respawn a child that exits unexpectedly (not as part of a shutdown we
    requested).

    Each child gets its own default worker id (hostname:pid:random, see
    Worker.__init__), so ids don't collide; an explicit --name is suffixed
    with its slot index for the same reason. Prometheus can only bind one
    port per process, so --metrics-port N gives child i port N + i.
    """
    ctx = multiprocessing.get_context("spawn")
    stop_event = ctx.Event()
    shutting_down = False

    def spawn(slot: int) -> multiprocessing.process.BaseProcess:
        child_kwargs: dict[str, Any] = dict(kwargs)
        if child_kwargs.get("name"):
            child_kwargs["name"] = f"{child_kwargs['name']}-{slot}"
        if base_metrics_port:
            child_kwargs["metrics_port"] = base_metrics_port + slot
        child_kwargs["stop_event"] = stop_event
        p = ctx.Process(target=_run_one_worker, kwargs=child_kwargs, daemon=False)
        p.start()
        return p

    procs: dict[int, multiprocessing.process.BaseProcess] = {
        slot: spawn(slot) for slot in range(num_workers)
    }

    def on_signal(signum: int, _frame: Any) -> None:
        nonlocal shutting_down
        shutting_down = True
        typer.echo(f"blitzq: shutting down {len(procs)} worker process(es)", err=True)
        stop_event.set()

    sigs = [signal.SIGINT, signal.SIGTERM]
    if hasattr(signal, "SIGBREAK"):
        sigs.append(signal.SIGBREAK)
    for sig in sigs:
        signal.signal(sig, on_signal)

    try:
        while procs:
            for slot, p in list(procs.items()):
                p.join(timeout=0.5)
                if p.is_alive():
                    continue
                del procs[slot]
                if shutting_down:
                    continue
                typer.echo(
                    f"blitzq: worker slot {slot} exited unexpectedly "
                    f"(code {p.exitcode}); restarting",
                    err=True,
                )
                procs[slot] = spawn(slot)
    except KeyboardInterrupt:
        stop_event.set()
        for p in procs.values():
            p.join()


@app.command()
def worker(
    app_path: Annotated[str, typer.Argument(metavar="APP", help="module:attribute of the Queue.")],
    queues: Annotated[
        str | None, typer.Option("--queues", "-Q", help="Comma-separated queues to consume.")
    ] = None,
    concurrency: Annotated[int, typer.Option("--concurrency", "-c", min=1)] = 100,
    queue_concurrency: Annotated[
        list[str] | None,
        typer.Option("--queue-concurrency", help="Per-queue limit, e.g. images=8 (repeatable)."),
    ] = None,
    workers: Annotated[
        int,
        typer.Option(
            "--workers",
            "-w",
            min=1,
            help="Number of worker processes to run (forked and supervised, like a prefork pool). "
            "Each still runs --concurrency async tasks internally.",
        ),
    ] = 1,
    batch_size: Annotated[int | None, typer.Option(help="Max messages per fetch.")] = None,
    threads: Annotated[int | None, typer.Option(help="Thread pool size for sync tasks.")] = None,
    processes: Annotated[
        int | None, typer.Option(help="Process pool size for executor='process' tasks.")
    ] = None,
    shutdown_timeout: Annotated[float, typer.Option(help="Seconds to drain on shutdown.")] = 30.0,
    promote: Annotated[
        bool, typer.Option("--promote/--no-promote", help="Promote due scheduled tasks.")
    ] = True,
    warn_cpu_bound: Annotated[
        bool,
        typer.Option(
            "--warn-cpu-bound/--no-warn-cpu-bound",
            help="Warn once per task name when a thread-executor task looks CPU-bound.",
        ),
    ] = True,
    include: Annotated[
        list[str] | None, typer.Option("--include", "-I", help="Extra modules to import.")
    ] = None,
    name: Annotated[str | None, typer.Option(help="Worker id.")] = None,
    metrics_port: Annotated[
        int | None, typer.Option(help="Serve Prometheus metrics on this port.")
    ] = None,
    log_level: Annotated[str, typer.Option(envvar="BLITZQ_LOG_LEVEL")] = "INFO",
    log_format: Annotated[str, typer.Option(help="text or json")] = "text",
) -> None:
    """Start a worker process (or, with --workers N, a supervised pool of N)."""
    kwargs = _WorkerKwargs(
        app_path=app_path,
        queues=[s.strip() for s in queues.split(",") if s.strip()] if queues else None,
        concurrency=concurrency,
        queue_concurrency=_parse_queue_concurrency(queue_concurrency or []),
        batch_size=batch_size,
        threads=threads,
        processes=processes,
        shutdown_timeout=shutdown_timeout,
        promote=promote,
        warn_cpu_bound=warn_cpu_bound,
        include=include or [],
        name=name,
        metrics_port=metrics_port,
        log_level=log_level,
        log_format=log_format,
    )
    if workers == 1:
        _run_one_worker(**kwargs)
    else:
        _run_worker_pool(workers, metrics_port, kwargs)


@app.command()
def scheduler(
    app_path: Annotated[str, typer.Argument(metavar="APP")],
    promote: Annotated[bool, typer.Option("--promote/--no-promote")] = True,
    poll_interval: Annotated[float, typer.Option()] = 1.0,
    include: Annotated[list[str] | None, typer.Option("--include", "-I")] = None,
    log_level: Annotated[str, typer.Option(envvar="BLITZQ_LOG_LEVEL")] = "INFO",
    log_format: Annotated[str, typer.Option()] = "text",
) -> None:
    """Run the periodic-task scheduler (safe to run several instances)."""
    from .scheduler import Scheduler, run_scheduler

    configure_logging(log_level, log_format)
    q = load_app(app_path)
    for mod in include or []:
        importlib.import_module(mod)
    run_scheduler(Scheduler(q, promote=promote, poll_interval=poll_interval))


@app.command()
def version() -> None:
    """Print the installed BlitzQ version."""
    typer.echo(__version__)


# -- queue --------------------------------------------------------------------------
@queue_app.command("stats")
def queue_stats(
    app_path: AppOpt = None,
    redis_url: UrlOpt = None,
    mode: ModeOpt = "reliable",
    namespace: NsOpt = "blitzq",
    as_json: JsonOpt = False,
) -> None:
    """Show queue depths, scheduled and dead-lettered counts, and live workers."""
    q = _client(app_path, redis_url, mode, namespace)

    async def go() -> dict[str, Any]:
        stats = await q.queue_stats()
        workers = await q.broker.workers()
        return {
            "mode": q.broker.guarantee,
            "queues": [
                {"name": s.name, "waiting": s.waiting, "in_progress": s.in_progress, **s.extra}
                for s in stats
            ],
            "scheduled": await q.broker.scheduled_count(),
            "dead_letters": await q.broker.dead_letter_count(),
            "workers": [msgspec.msgpack.decode(v) for v in workers.values()],
        }

    data = _run(q, go)
    if as_json:
        typer.echo(json.dumps(data, indent=2, default=str))
        return
    typer.echo(f"delivery: {data['mode']}")
    typer.echo(f"{'QUEUE':<24}{'WAITING':>10}{'IN PROGRESS':>14}")
    for s in data["queues"]:
        ip = "-" if s["in_progress"] is None else s["in_progress"]
        typer.echo(f"{s['name']:<24}{s['waiting']:>10}{ip:>14}")
    typer.echo(f"scheduled: {data['scheduled']}   dead letters: {data['dead_letters']}")
    typer.echo(f"workers: {len(data['workers'])}")
    now = time.time()
    for w in data["workers"]:
        typer.echo(
            f"  {w['id']}  queues={','.join(w['queues'])}  running={w['running']}/"
            f"{w['concurrency']}  processed={w['processed']}  seen={now - w['seen_at']:.1f}s ago"
        )


@queue_app.command("purge")
def queue_purge(
    queue: Annotated[str, typer.Argument()],
    yes: Annotated[bool, typer.Option("--yes", help="Confirm deletion.")] = False,
    app_path: AppOpt = None,
    redis_url: UrlOpt = None,
    mode: ModeOpt = "reliable",
    namespace: NsOpt = "blitzq",
) -> None:
    """Delete all waiting messages in QUEUE."""
    if not yes:
        typer.echo("refusing to purge without --yes", err=True)
        raise typer.Exit(2)
    q = _client(app_path, redis_url, mode, namespace)
    n = _run(q, lambda: q.purge(queue))
    typer.echo(f"purged {n} messages from {queue}")


# -- task ---------------------------------------------------------------------------
@task_app.command("inspect")
def task_inspect(
    task_id: Annotated[str, typer.Argument()],
    app_path: AppOpt = None,
    redis_url: UrlOpt = None,
    mode: ModeOpt = "reliable",
    namespace: NsOpt = "blitzq",
    as_json: JsonOpt = False,
) -> None:
    """Show the state, timings, result and error of a task."""
    q = _client(app_path, redis_url, mode, namespace)
    info = _run(q, lambda: q.inspect(task_id))
    if info is None:
        typer.echo(
            f"no record for task {task_id} (queued/running without state tracking, expired, "
            "or unknown)",
            err=True,
        )
        raise typer.Exit(1)
    if as_json:
        typer.echo(json.dumps(_to_builtins(info), indent=2, default=str))
        return
    typer.echo(f"id:          {info.id}")
    typer.echo(f"state:       {info.state.value}")
    typer.echo(f"task:        {info.task or '-'}")
    typer.echo(f"queue:       {info.queue or '-'}")
    typer.echo(f"attempt:     {info.attempt}/{info.max_attempts or '-'}")
    typer.echo(f"created:     {_ts(info.created_at)}")
    typer.echo(f"enqueued:    {_ts(info.enqueued_at)}")
    typer.echo(f"started:     {_ts(info.started_at)}")
    typer.echo(f"finished:    {_ts(info.finished_at)}")
    if info.duration is not None:
        typer.echo(f"duration:    {info.duration:.6f}s")
    if info.next_attempt_at:
        typer.echo(f"next run:    {_ts(info.next_attempt_at)}")
    if info.correlation_id:
        typer.echo(f"correlation: {info.correlation_id}")
    if info.error:
        typer.echo(f"error:       {info.error.type}: {info.error.message}")
    if info.result is not None:
        typer.echo(f"result:      {info.result!r}")


@task_app.command("retry")
def task_retry(
    task_id: Annotated[str, typer.Argument()],
    app_path: AppOpt = None,
    redis_url: UrlOpt = None,
    mode: ModeOpt = "reliable",
    namespace: NsOpt = "blitzq",
) -> None:
    """Re-enqueue a dead-lettered task with a fresh attempt budget."""
    q = _client(app_path, redis_url, mode, namespace)
    if _run(q, lambda: q.retry(task_id)):
        typer.echo(f"re-enqueued {task_id}")
    else:
        typer.echo(f"no dead letter with id {task_id}", err=True)
        raise typer.Exit(1)


@task_app.command("cancel")
def task_cancel(
    task_id: Annotated[str, typer.Argument()],
    app_path: AppOpt = None,
    redis_url: UrlOpt = None,
    mode: ModeOpt = "reliable",
    namespace: NsOpt = "blitzq",
) -> None:
    """Cancel a scheduled task, or revoke a queued/running one (best effort)."""
    q = _client(app_path, redis_url, mode, namespace)
    if _run(q, lambda: q.cancel(task_id)):
        typer.echo(f"cancelled scheduled task {task_id}")
    else:
        typer.echo(
            f"revocation recorded for {task_id}; workers will skip it if not yet started "
            "and cancel it if it is a running async task"
        )


# -- dead letters -------------------------------------------------------------------
@dlq_app.command("list")
def dlq_list(
    limit: Annotated[int, typer.Option(min=1)] = 50,
    offset: Annotated[int, typer.Option(min=0)] = 0,
    queue: Annotated[str | None, typer.Option(help="Only this queue.")] = None,
    app_path: AppOpt = None,
    redis_url: UrlOpt = None,
    mode: ModeOpt = "reliable",
    namespace: NsOpt = "blitzq",
    as_json: JsonOpt = False,
) -> None:
    """List dead-lettered tasks, newest first."""
    q = _client(app_path, redis_url, mode, namespace)

    async def go() -> tuple[int, list[Any]]:
        return await q.broker.dead_letter_count(), await q.dead_letters(limit, offset)

    total, items = _run(q, go)
    if queue:
        items = [d for d in items if d.queue == queue]
    if as_json:
        rows = []
        for d in items:
            row = _to_builtins(d)
            row.pop("message", None)
            rows.append(row)
        typer.echo(json.dumps({"total": total, "items": rows}, indent=2, default=str))
        return
    typer.echo(f"{total} dead-lettered task(s)")
    for d in items:
        err = f"{d.error.type}: {d.error.message}" if d.error else "-"
        typer.echo(
            f"{_ts(d.failed_at)}  {d.id}  queue={d.queue}  task={d.task or '-'}  "
            f"attempt={d.attempt}  reason={d.reason}  error={err[:120]}"
        )


@dlq_app.command("purge")
def dlq_purge(
    yes: Annotated[bool, typer.Option("--yes")] = False,
    app_path: AppOpt = None,
    redis_url: UrlOpt = None,
    mode: ModeOpt = "reliable",
    namespace: NsOpt = "blitzq",
) -> None:
    """Delete all dead letters."""
    if not yes:
        typer.echo("refusing to purge without --yes", err=True)
        raise typer.Exit(2)
    q = _client(app_path, redis_url, mode, namespace)
    n = _run(q, q.broker.purge_dead_letters)
    typer.echo(f"deleted {n} dead letters")


# -- benchmarks ---------------------------------------------------------------------
def _bench_module(name: str) -> Any:
    if os.getcwd() not in sys.path:
        sys.path.insert(0, os.getcwd())
    try:
        return importlib.import_module(f"benchmarks.{name}")
    except ImportError as exc:
        typer.echo(
            f"cannot import benchmarks.{name} ({exc}). Benchmark commands must be run from a "
            'BlitzQ repository checkout with `pip install -e ".[bench]"`.',
            err=True,
        )
        raise typer.Exit(2) from exc


@bench_app.command("run")
def bench_run(
    config: Annotated[str, typer.Option("--config", help="Benchmark YAML config.")],
    output: Annotated[str, typer.Option("--output", help="Results directory.")] = (
        "benchmarks/results"
    ),
    systems: Annotated[
        str | None, typer.Option(help="Comma-separated subset of systems to run.")
    ] = None,
    repetitions: Annotated[int | None, typer.Option(help="Override repetitions.")] = None,
) -> None:
    """Run a benchmark configuration against the configured systems."""
    runner = _bench_module("runner")
    runner.run_config(
        config,
        output,
        systems=systems.split(",") if systems else None,
        repetitions=repetitions,
    )


@bench_app.command("compare")
def bench_compare(
    input_dir: Annotated[str, typer.Option("--input", help="Results directory.")] = (
        "benchmarks/results"
    ),
    output: Annotated[str | None, typer.Option(help="Report path (Markdown).")] = None,
) -> None:
    """Aggregate raw results into CSV/JSON summaries, charts and a Markdown report."""
    report = _bench_module("report")
    path = report.build_report(input_dir, output)
    typer.echo(f"report written to {path}")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
