# Roadmap

What's intentionally out of scope for 1.0.0, why, and what would need to change
to add it. Nothing here is scheduled or promised; it's a record of known gaps
and the reasoning behind them, so the [comparison report](COMPARISON_REPORT.md)
and [current limitations](../README.md#current-limitations) don't read as
oversights.

## Redis Cluster support

**Status:** not supported. A single Redis primary (optionally with replicas)
is the only supported topology; see [operations.md](operations.md).

**Why it isn't already supported:** reliable mode's crash-recovery guarantees
depend on operations that are straightforward on a single Redis node but
don't translate directly to Cluster:

- The schedule promoter (`PROMOTE`, a Lua script) atomically moves due
  entries from one sorted set into per-queue streams/lists in a single
  server-side transaction. Lua scripts on Redis Cluster can only touch keys
  that hash to the same slot, so the schedule key and every queue it
  promotes into would need to be co-located — not guaranteed once queue
  names are arbitrary and sharded.
- Queue keys, the schedule, the dead-letter store and per-task records are
  presently plain keys under one namespace; Cluster requires either hash
  tags to force co-location (constraining key design and rebalancing) or
  moving the atomicity these scripts provide into client-side coordination
  (more round-trips, harder to reason about, more surface for subtle bugs).
- None of this is impossible — it's a real, multi-week design and
  implementation project, not a missing flag — and BlitzQ shipped 1.0 with
  the smaller, provably-correct single-node design deliberately, rather than
  delay the release for scale most users don't need yet.

**What would make this happen:** a concrete need from someone running Redis
Cluster at a scale a single primary can't handle. If that's you, open an
issue with your throughput/dataset numbers — it changes the design tradeoffs
above from theoretical to load-bearing.

## Other known gaps

Carried over from [current limitations](../README.md#current-limitations) for
visibility; none of these have a design in progress.

- **Task chains/groups/chords** (a task that runs after a set of others
  completes, or a pipeline of dependent tasks). BlitzQ currently only
  composes tasks by having one task enqueue the next from inside its body.
- **A web dashboard** (a Flower-equivalent). Today: CLI inspection
  (`blitzq task inspect`, `blitzq queue stats`) and the Prometheus exporter.
- **Valkey, Dragonfly, KeyDB** are untested against BlitzQ's Lua scripts and
  Streams usage. They may work; they aren't declared supported because
  nothing here has verified it.

## Already fixed

- ~~`get_result()` polls for completion instead of being notified~~ — fixed:
  Redis brokers now publish on the task's record channel when its final
  state is written, and `get_result()` waits on that instead of polling
  tightly, with a coarse poll as a safety net. See
  [delivery_guarantees.md](delivery_guarantees.md#results-and-task-state).
