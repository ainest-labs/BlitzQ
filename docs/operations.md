# Operations

## Processes to run

| Process | Command | How many |
|---|---|---|
| Web / producers | your application | any |
| Workers | `blitzq worker module:app --queues ... --concurrency N` | scale per queue |
| Scheduler | `blitzq scheduler module:app` | only if you use periodic tasks; run 2+ for availability (occurrences are deduplicated) |

Workers also promote delayed tasks and retries (`--no-promote` disables it), so a
scheduler process is optional for delayed tasks.

## Signals and shutdown

`SIGTERM` or `SIGINT` (Ctrl-C) triggers a graceful shutdown. The worker stops
fetching, waits up to `--shutdown-timeout` (30 s) for running tasks, then cancels
the rest and returns their messages to the queue. A second signal skips the wait.
Give orchestrators a termination grace period longer than `shutdown_timeout`
(Kubernetes: `terminationGracePeriodSeconds`).

On Windows, Ctrl-C and Ctrl-Break are handled. SIGTERM from other processes is
not deliverable to console apps, so use Ctrl-Break or a service manager there.

## Inspecting a deployment

```bash
blitzq queue stats --app myapp.tasks:app          # queue depth, in-progress, scheduled, DLQ, workers
blitzq task inspect <id> --app myapp.tasks:app    # state, attempts, timings, error
blitzq task cancel <id> --app ...                 # certain for scheduled tasks, best effort otherwise
blitzq dead-letter list --app ...                 # newest first
blitzq dead-letter summary --by error_type --app ...   # what is failing, grouped
blitzq task retry <id> --app ...                  # replay one dead letter with a fresh attempt budget
blitzq dead-letter retry-all --yes --app ...      # replay many (filtered); see below
blitzq dead-letter purge --yes --app ...          # delete all, or only those matching filters
blitzq queue purge <queue> --yes --app ...
```

### Working with many dead letters

`list`, `summary`, `retry-all` and `purge` share the same filters. Every filter
you give must match:

| Option | Matches |
|---|---|
| `--task billing.*` | task name or glob |
| `--queue emails` | the queue and its priority levels |
| `--error-type GatewayTimeout` | exception class name |
| `--error-contains "timed out"` | text in the error message (case-insensitive) |
| `--reason "max attempts exceeded"` | exact dead-letter reason |
| `--header country=IN` (repeatable, `-H`) | headers set when the task was enqueued |
| `--rate-key stripe:IN` | the call's rate key |
| `--correlation-id ID` | the task's correlation id |
| `--since 2h` / `--until 30m` | failure time (`30m`, `2h`, `1d`, or an ISO timestamp) |

Selecting by branch, tenant or country works through headers: enqueue with
`task.options(headers={"country": "IN", "branch": "b12"})` and filter on them.

```bash
blitzq dead-letter summary --by error_type --since 1d          # what failed, and how often
blitzq dead-letter summary --by header:country                 # ...or where
blitzq dead-letter list --task 'billing.*' -H country=IN --error-type GatewayTimeout
blitzq dead-letter retry-all --task 'billing.*' -H country=IN --dry-run    # preview
blitzq dead-letter retry-all --task 'billing.*' -H country=IN --rate 50 --yes
blitzq dead-letter purge --error-type CardDeclined --since 7d --yes
```

`retry-all` re-enqueues oldest first, each with a fresh attempt budget, and takes
`--limit N` and `--rate N` (publishes per second, so a large replay does not
hammer the service that caused the failures). Replaying one entry is atomic, so
two operators running it at once, or a replay racing a purge, never publish an
entry twice; an entry someone else already handled is counted as `skipped`.
Entries whose message can no longer be decoded stay in the store and are reported
as `undecodable`. Scans work on a snapshot of the matching ids and fetch in chunks
of 200, so large dead-letter sets are safe to process.

The same operations are available in Python:

```python
await queue.dead_letters(task="billing.*", headers={"country": "IN"}, limit=50)
await queue.dead_letter_summary(by="header:country")
result = await queue.retry_dead_letters(error_type="GatewayTimeout", rate=50)
print(result.requeued, result.skipped)           # BulkResult
await queue.purge_dead_letters(error_type="CardDeclined", dry_run=True)
```

Instead of `--app` you can pass `--redis-url`, `--mode` and `--namespace`, or set
the environment variables `BLITZQ_APP`, `BLITZQ_REDIS_URL`, `BLITZQ_MODE` and
`BLITZQ_NAMESPACE`. Add `--json` for machine-readable output.

## Metrics and logs

- Each worker keeps per-queue counters (`received`, `started`, `succeeded`,
  `failed`, `retried`, `dead_lettered`, `timeouts`, `cancelled`, `recovered`,
  `requeued`, `malformed`) and histograms for execution time and
  enqueue-to-start latency. Labels are the queue name only, never task ids.
- Workers publish a snapshot to Redis every 2 s; `blitzq queue stats --json`
  includes it.
- `--metrics-port 9100` serves Prometheus metrics (`pip install "blitzq[monitoring]"`):
  `blitzq_tasks_total{queue,event}`, `blitzq_task_duration_seconds`,
  `blitzq_queue_latency_seconds`.
- `--log-format json` emits one JSON object per line with fields such as
  `task_id`, `task`, `queue` and `attempt`. Task arguments and results are never
  logged. Exception messages are logged, so don't put secrets in exception text.

## Redis configuration

- **Persistence decides durability.** With `appendonly yes` and
  `appendfsync everysec`, a Redis crash can lose about the last second of
  enqueues, acks and results. With RDB only, everything since the last snapshot
  can be lost. With no persistence, a restart loses all queued, scheduled and
  in-flight work. Replication to a replica is asynchronous, so a failover can lose
  recently acknowledged writes too.
- **Memory:** set `maxmemory` with `maxmemory-policy noeviction`. An eviction
  policy would silently delete queued messages and records. With `noeviction`,
  Redis rejects writes when full, so enqueues fail loudly instead.
- **Keys** live under the namespace (`blitzq:` by default). Records expire after
  `result_ttl`. Dead letters are capped at `dead_letter_max` (100 000; oldest
  dropped). Streams shrink as messages are acknowledged (`XDEL`).
- **Redis Cluster is not supported.** Use a single primary (optionally with
  replicas and Sentinel).

## Security

- Nothing received from Redis is unpickled or executed. Messages are MessagePack
  or JSON decoded into plain data with msgspec, then validated against the
  envelope schema. Malformed messages are dead-lettered. Messages larger than
  `Serializer(max_message_size=...)` (default 8 MiB) are rejected on enqueue and
  receipt.
- Anyone who can write to your Redis can enqueue any *registered* task with
  arbitrary plain-data arguments. Protect Redis with authentication (ACL user and
  password in the URL), TLS (`rediss://`) and network isolation, and validate
  task arguments like any other untrusted input.
- Task results may contain sensitive data. Don't expose `get_result`/`inspect`
  over HTTP without application-level authorization.
- Retries can amplify load on a struggling dependency. Use `max_delay`, jitter, a
  bounded number of retries and `dont_retry_on` for permanent errors. Per-queue
  concurrency caps how hard a queue can hit a downstream service.

## Timeouts and hung tasks

`timeout=` cancels `async` tasks. Thread and process tasks cannot be killed: the
worker stops waiting and records a timeout, but the call keeps its concurrency
slot until it returns. A task that blocks forever therefore permanently consumes
one slot. Monitor `running` in `queue stats` and restart such workers.

## Upgrading

The message format is versioned by position: new fields are appended with
defaults, so workers of version *N+1* read messages of version *N*. Deploy new
workers before producers when adding tasks, so messages for new task names find
a worker that knows them. Messages for unknown tasks are dead-lettered (reason
`unknown task`) and can be replayed with `blitzq task retry`.
