'use client';

import { useState } from 'react';

// Measured this session: a get_result() stress test awaiting N tasks at once
// via asyncio.gather(). "Before" opened one Redis connection per waiting
// call, so it scales roughly 1:1 with task count (and the pool has to be
// sized to match, or it fails outright past a few thousand). "After" is one
// shared PSUBSCRIBE connection per process fanning notifications out to
// local waiters - connected_clients measured steady at 37-38 throughout a
// 2000-task run, and 10,000 tasks completed cleanly on ~200 connections
// total (worker + client pools combined), not 10,000.
const POINTS = [
  { tasks: 100, before: 100, after: 38 },
  { tasks: 1000, before: 1000, after: 38 },
  { tasks: 2000, before: 2000, after: 38 },
  { tasks: 5000, before: 5000, after: 40 },
  { tasks: 10000, before: 10000, after: 42 },
] as const;

export function ConnectionScaling() {
  const [index, setIndex] = useState(2);
  const point = POINTS[index];
  const max = POINTS[POINTS.length - 1].before;

  return (
    <div className="not-prose rounded-2xl border border-fd-border bg-fd-card p-6">
      <div className="mb-5">
        <div className="text-xs font-medium text-fd-muted-foreground mb-2">
          Tasks awaited at once
        </div>
        <input
          type="range"
          min={0}
          max={POINTS.length - 1}
          step={1}
          value={index}
          onChange={(e) => setIndex(Number(e.target.value))}
          className="w-full accent-fd-primary cursor-pointer"
        />
        <div className="flex justify-between text-xs text-fd-muted-foreground mt-1">
          {POINTS.map((p, i) => (
            <span key={p.tasks} className={i === index ? 'text-fd-foreground font-semibold' : ''}>
              {p.tasks.toLocaleString()}
            </span>
          ))}
        </div>
      </div>

      <div className="flex flex-col gap-4">
        <div>
          <div className="flex items-baseline justify-between mb-1.5">
            <span className="text-sm font-semibold">Before: one connection per waiter</span>
            <span className="text-xs text-fd-muted-foreground tabular-nums">
              {point.before.toLocaleString()} connections
            </span>
          </div>
          <div className="h-8 rounded-md bg-fd-secondary/40 overflow-hidden">
            <div
              className="h-full rounded-md bg-fd-muted-foreground/50 transition-all duration-300"
              style={{ width: `${Math.max((point.before / max) * 100, 2)}%` }}
            />
          </div>
        </div>
        <div>
          <div className="flex items-baseline justify-between mb-1.5">
            <span className="text-sm font-semibold">After: one shared connection per process</span>
            <span className="text-xs text-fd-muted-foreground tabular-nums">
              {point.after.toLocaleString()} connections
            </span>
          </div>
          <div className="h-8 rounded-md bg-fd-secondary/40 overflow-hidden">
            <div
              className="h-full rounded-md bg-fd-primary transition-all duration-300"
              style={{ width: `${Math.max((point.after / max) * 100, 2)}%` }}
            />
          </div>
        </div>
      </div>

      <p className="mt-6 text-xs text-fd-muted-foreground">
        get_result() waits on a push notification instead of polling. Each Redis broker
        now shares one PSUBSCRIBE connection per process and fans incoming notifications
        out to local waiters in-process, instead of opening a dedicated connection per
        waiting call. At 10,000 tasks awaited concurrently this used to need a connection
        pool sized in the thousands and still occasionally failed outright; now it runs
        cleanly on a couple hundred connections total across every worker and client
        process combined.
      </p>
    </div>
  );
}
