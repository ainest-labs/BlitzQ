'use client';

import { useEffect, useRef, useState } from 'react';

// Measured tasks/sec at equal settings (1 worker process, 16 slots) from the
// comparison report - real numbers, only the animation's wall-clock speed is
// scaled down so a race is watchable instead of instant.
interface WorkloadDef {
  label: string;
  hint: string;
  values: { BlitzQ: number; Celery: number };
  processValues?: { BlitzQ: number; Celery: number };
  processHint?: string;
}

const WORKLOADS: Record<'noop' | 'io' | 'retry' | 'cpu' | 'pool', WorkloadDef> = {
  noop: {
    label: 'No-op task',
    hint: 'Bare function call - pure dispatch overhead.',
    values: { BlitzQ: 2604, Celery: 855 },
  },
  io: {
    label: '20ms I/O task',
    hint: 'Simulated network/database call inside the task.',
    values: { BlitzQ: 755, Celery: 720 },
  },
  retry: {
    label: 'Retry-heavy (50% fail once)',
    hint: 'Half the tasks fail once and get retried with backoff.',
    values: { BlitzQ: 1745, Celery: 497 },
  },
  cpu: {
    label: 'CPU-bound task',
    hint: "BlitzQ's honest weak spot: the default thread executor is GIL-bound.",
    values: { BlitzQ: 73, Celery: 486 },
    processValues: { BlitzQ: 472, Celery: 445 },
    processHint:
      'With executor="process" (8 worker processes each), BlitzQ ties Celery\'s prefork pool.',
  },
  pool: {
    label: 'Worker pool (2000 tasks)',
    hint: 'blitzq worker --workers 4 --concurrency 25 vs Celery prefork, concurrency=16 - a real HTTP-call task, measured end-to-end on the same machine (separate from the equal-settings numbers above).',
    values: { BlitzQ: 328.8, Celery: 283.2 },
  },
};

type WorkloadKey = keyof typeof WORKLOADS;
type SystemName = 'BlitzQ' | 'Celery';

const TASK_COUNTS = [200, 1000, 5000] as const;

// Animation wall-clock budget: the slower system's bar takes this long to fill.
const RACE_DURATION_MS = 4000;

interface RaceState {
  status: 'idle' | 'running' | 'done';
  startedAt: number;
  finishedAt: Partial<Record<SystemName, number>>;
}

export function LiveDemo() {
  const [workload, setWorkload] = useState<WorkloadKey>('noop');
  const [taskCount, setTaskCount] = useState<number>(TASK_COUNTS[1]);
  const [processExecutor, setProcessExecutor] = useState(false);
  const [race, setRace] = useState<RaceState>({ status: 'idle', startedAt: 0, finishedAt: {} });
  const [progress, setProgress] = useState<Record<SystemName, number>>({ BlitzQ: 0, Celery: 0 });
  const frame = useRef<number | null>(null);

  const workloadDef = WORKLOADS[workload];
  const useProcess = workload === 'cpu' && processExecutor;
  const values = useProcess ? workloadDef.processValues! : workloadDef.values;
  const hint = useProcess ? workloadDef.processHint! : workloadDef.hint;
  const slowest = Math.min(values.BlitzQ, values.Celery);
  // Real time each system would take to clear the queue, relative to the slowest.
  const realDurationMs: Record<SystemName, number> = {
    BlitzQ: (taskCount / values.BlitzQ / (taskCount / slowest)) * RACE_DURATION_MS,
    Celery: (taskCount / values.Celery / (taskCount / slowest)) * RACE_DURATION_MS,
  };

  function runRace() {
    if (frame.current) cancelAnimationFrame(frame.current);
    const startedAt = performance.now();
    setProgress({ BlitzQ: 0, Celery: 0 });
    setRace({ status: 'running', startedAt, finishedAt: {} });

    const tick = () => {
      const elapsed = performance.now() - startedAt;
      const next: Record<SystemName, number> = {
        BlitzQ: Math.min(elapsed / realDurationMs.BlitzQ, 1),
        Celery: Math.min(elapsed / realDurationMs.Celery, 1),
      };
      setProgress(next);

      setRace((prev) => {
        const finishedAt = { ...prev.finishedAt };
        (['BlitzQ', 'Celery'] as SystemName[]).forEach((sys) => {
          if (next[sys] >= 1 && finishedAt[sys] === undefined) finishedAt[sys] = elapsed;
        });
        const allDone = finishedAt.BlitzQ !== undefined && finishedAt.Celery !== undefined;
        return { ...prev, finishedAt, status: allDone ? 'done' : 'running' };
      });

      if (next.BlitzQ < 1 || next.Celery < 1) {
        frame.current = requestAnimationFrame(tick);
      }
    };
    frame.current = requestAnimationFrame(tick);
  }

  function reset() {
    if (frame.current) cancelAnimationFrame(frame.current);
    setRace({ status: 'idle', startedAt: 0, finishedAt: {} });
    setProgress({ BlitzQ: 0, Celery: 0 });
  }

  // Changing the workload/count/executor mid-run would desync the animation;
  // reset the plain state during render, per React's "adjusting state when a
  // prop changes" pattern (refs aren't allowed to be touched during render,
  // so the animation-frame cleanup below stays in an effect).
  const raceKey = `${workload}-${taskCount}-${processExecutor}`;
  const [prevRaceKey, setPrevRaceKey] = useState(raceKey);
  if (prevRaceKey !== raceKey) {
    setPrevRaceKey(raceKey);
    setRace({ status: 'idle', startedAt: 0, finishedAt: {} });
    setProgress({ BlitzQ: 0, Celery: 0 });
  }

  useEffect(() => {
    if (frame.current) {
      cancelAnimationFrame(frame.current);
      frame.current = null;
    }
  }, [raceKey]);

  useEffect(() => {
    return () => {
      if (frame.current) cancelAnimationFrame(frame.current);
    };
  }, []);

  const speedup = values.BlitzQ / values.Celery;
  const blitzqWins = speedup >= 1;

  return (
    <div className="not-prose rounded-2xl border border-fd-border bg-fd-card p-6">
      <div className="grid gap-4 sm:grid-cols-2 mb-6">
        <div>
          <div className="text-xs font-medium text-fd-muted-foreground mb-2">Workload</div>
          <div className="flex flex-wrap gap-2">
            {(Object.keys(WORKLOADS) as WorkloadKey[]).map((key) => (
              <button
                key={key}
                onClick={() => setWorkload(key)}
                className={`rounded-full border px-3 py-1.5 text-xs font-medium transition-colors cursor-pointer ${
                  workload === key
                    ? 'border-fd-primary bg-fd-primary text-fd-primary-foreground'
                    : 'border-fd-border text-fd-muted-foreground hover:text-fd-foreground'
                }`}
              >
                {WORKLOADS[key].label}
              </button>
            ))}
          </div>
        </div>
        <div>
          <div className="text-xs font-medium text-fd-muted-foreground mb-2">Tasks to run</div>
          <div className="flex flex-wrap gap-2">
            {TASK_COUNTS.map((count) => (
              <button
                key={count}
                onClick={() => setTaskCount(count)}
                className={`rounded-full border px-3 py-1.5 text-xs font-medium transition-colors cursor-pointer ${
                  taskCount === count
                    ? 'border-fd-primary bg-fd-primary text-fd-primary-foreground'
                    : 'border-fd-border text-fd-muted-foreground hover:text-fd-foreground'
                }`}
              >
                {count.toLocaleString()}
              </button>
            ))}
          </div>
        </div>
      </div>

      {workload === 'cpu' && (
        <div className="mb-4 rounded-lg border border-fd-border bg-fd-secondary/20 p-3">
          <p className="text-xs text-fd-muted-foreground mb-3">
            By default a BlitzQ task runs on a thread pool, and threads share one Python
            interpreter - a CPU-heavy task blocks the others (the GIL). Passing{' '}
            <code className="text-fd-foreground">@queue.task(executor=&quot;process&quot;)</code>{' '}
            moves that task onto a separate worker process instead, one per CPU core, so
            CPU-bound work actually runs in parallel - matching what Celery&apos;s prefork
            pool does by default.
          </p>
          <label className="flex items-center gap-3 cursor-pointer select-none w-fit">
            <span
              className={`relative inline-flex h-6 w-11 shrink-0 items-center rounded-full border-2 transition-colors ${
                processExecutor
                  ? 'bg-fd-primary border-fd-primary'
                  : 'bg-fd-muted-foreground/40 border-fd-muted-foreground/40'
              }`}
            >
              <input
                type="checkbox"
                checked={processExecutor}
                onChange={(e) => setProcessExecutor(e.target.checked)}
                className="sr-only"
              />
              <span
                className={`inline-block h-4 w-4 transform rounded-full bg-white ring-1 ring-black/10 shadow-md transition-transform ${
                  processExecutor ? 'translate-x-[22px]' : 'translate-x-1'
                }`}
              />
            </span>
            <span className="text-xs font-semibold">
              Use <code className="text-fd-foreground">executor=&quot;process&quot;</code>
            </span>
          </label>
        </div>
      )}

      <p className="text-xs text-fd-muted-foreground mb-5">{hint}</p>

      <div className="flex flex-col gap-4 mb-5">
        {(['BlitzQ', 'Celery'] as SystemName[]).map((sys) => {
          const pct = Math.round(progress[sys] * 1000) / 10;
          const completedCount = Math.min(Math.floor(progress[sys] * taskCount), taskCount);
          const finishedAt = race.finishedAt[sys];
          return (
            <div key={sys}>
              <div className="flex items-baseline justify-between mb-1.5">
                <span className="text-sm font-semibold">{sys}</span>
                <span className="text-xs text-fd-muted-foreground tabular-nums">
                  {finishedAt !== undefined
                    ? `finished in ${(finishedAt / 1000).toFixed(2)}s (simulated)`
                    : race.status === 'running'
                      ? `${completedCount.toLocaleString()}/${taskCount.toLocaleString()} tasks`
                      : `${taskCount.toLocaleString()} tasks queued`}
                </span>
              </div>
              <div className="h-8 rounded-md bg-fd-secondary/40 overflow-hidden">
                <div
                  className={`h-full rounded-md ${sys === 'BlitzQ' ? 'bg-fd-primary' : 'bg-fd-muted-foreground/50'}`}
                  style={{
                    width: `${pct}%`,
                    transition: race.status === 'idle' ? 'none' : 'width 80ms linear',
                  }}
                />
              </div>
            </div>
          );
        })}
      </div>

      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex gap-2">
          <button
            onClick={runRace}
            disabled={race.status === 'running'}
            className="rounded-full bg-fd-primary text-fd-primary-foreground px-4 py-1.5 text-xs font-semibold cursor-pointer disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {race.status === 'running' ? 'Running…' : 'Run race'}
          </button>
          {race.status !== 'idle' && (
            <button
              onClick={reset}
              className="rounded-full border border-fd-border px-4 py-1.5 text-xs font-semibold cursor-pointer"
            >
              Reset
            </button>
          )}
        </div>

        {race.status === 'done' ? (
          <p className="text-sm font-medium">
            BlitzQ was{' '}
            <span className="text-fd-primary">
              {(blitzqWins ? speedup : 1 / speedup).toFixed(1)}x {blitzqWins ? 'faster' : 'slower'}
            </span>{' '}
            on this workload ({values.BlitzQ.toLocaleString()} vs {values.Celery.toLocaleString()}{' '}
            tasks/sec, measured).
          </p>
        ) : (
          <p className="text-xs text-fd-muted-foreground">
            Bars race at the real measured throughput ratio, time-compressed to fit the screen.
          </p>
        )}
      </div>
    </div>
  );
}
