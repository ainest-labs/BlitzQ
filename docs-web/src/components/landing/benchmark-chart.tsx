'use client';

import { useState } from 'react';
import Link from 'next/link';

interface Dataset {
  key: string;
  label: string;
  unit: string;
  lowerIsBetter?: boolean;
  values: { BlitzQ: number; Celery: number; Huey: number };
}

const datasets: Dataset[] = [
  {
    key: 'noop-equal',
    label: 'No-op tasks - equal settings',
    unit: 'tasks/sec',
    values: { BlitzQ: 2604, Celery: 855, Huey: 1499 },
  },
  {
    key: 'noop-tuned',
    label: 'No-op tasks - each system tuned',
    unit: 'tasks/sec',
    values: { BlitzQ: 63542, Celery: 3139, Huey: 6024 },
  },
  {
    key: 'io-tuned',
    label: '20ms I/O task - each system tuned',
    unit: 'tasks/sec',
    values: { BlitzQ: 41807, Celery: 2632, Huey: 8657 },
  },
  {
    key: 'retry',
    label: 'Retry-heavy (50% fail once) - tuned',
    unit: 'tasks/sec',
    values: { BlitzQ: 8577, Celery: 1370, Huey: 1956 },
  },
  {
    key: 'cpu-thread',
    label: 'CPU-bound - default thread executor',
    unit: 'tasks/sec',
    values: { BlitzQ: 73, Celery: 486, Huey: 70 },
  },
  {
    key: 'cpu-process',
    label: 'CPU-bound - process pools, 8 each',
    unit: 'tasks/sec',
    values: { BlitzQ: 472, Celery: 445, Huey: 478 },
  },
  {
    key: 'memory',
    label: 'Worker memory footprint (no-op)',
    unit: 'MB RSS',
    lowerIsBetter: true,
    values: { BlitzQ: 38.8, Celery: 775.2, Huey: 42.9 },
  },
];

const systems = ['BlitzQ', 'Celery', 'Huey'] as const;

const colors: Record<(typeof systems)[number], string> = {
  BlitzQ: 'bg-fd-primary',
  Celery: 'bg-fd-muted-foreground/50',
  Huey: 'bg-fd-muted-foreground/25',
};

function formatValue(value: number) {
  if (value >= 1000) return value.toLocaleString('en-US', { maximumFractionDigits: 0 });
  return value.toLocaleString('en-US', { maximumFractionDigits: 1 });
}

// Log scale so a 63,542 vs 3,139 gap is still readable next to a 73 vs 486 one.
function barWidth(value: number, max: number) {
  const pct = (Math.log10(value + 1) / Math.log10(max + 1)) * 100;
  return Math.max(Math.round(pct * 100) / 100, 3);
}

export function BenchmarkChart() {
  const [activeKey, setActiveKey] = useState(datasets[1].key);
  const active = datasets.find((d) => d.key === activeKey)!;
  const max = Math.max(...Object.values(active.values));

  return (
    <div className="not-prose rounded-2xl border border-fd-border bg-fd-card p-6">
      <div className="flex flex-wrap gap-2 mb-6">
        {datasets.map((d) => (
          <button
            key={d.key}
            onClick={() => setActiveKey(d.key)}
            className={`rounded-full border px-3 py-1.5 text-xs font-medium transition-colors cursor-pointer ${
              d.key === activeKey
                ? 'border-fd-primary bg-fd-primary text-fd-primary-foreground'
                : 'border-fd-border text-fd-muted-foreground hover:text-fd-foreground'
            }`}
          >
            {d.label}
          </button>
        ))}
      </div>

      <div className="flex flex-col gap-4">
        {systems.map((system) => {
          const value = active.values[system];
          const isWinner = active.lowerIsBetter
            ? value === Math.min(...Object.values(active.values))
            : value === max;
          return (
            <div key={system} className="flex items-center gap-4">
              <div className="w-16 shrink-0 text-sm font-medium">{system}</div>
              <div className="flex-1 h-7 rounded-md bg-fd-secondary/40 overflow-hidden">
                <div
                  className={`h-full rounded-md transition-all duration-500 ease-out ${colors[system]}`}
                  style={{ width: `${barWidth(value, max)}%` }}
                />
              </div>
              <div className="w-28 shrink-0 text-right text-sm tabular-nums">
                {formatValue(value)} <span className="text-fd-muted-foreground">{active.unit}</span>
                {isWinner ? <span className="ml-1 text-fd-primary">★</span> : null}
              </div>
            </div>
          );
        })}
      </div>

      <p className="mt-6 text-xs text-fd-muted-foreground">
        Median of 5 runs, synthetic task bodies, {active.lowerIsBetter ? 'lower is better' : 'higher is better'}.
        BlitzQ is not fastest everywhere - CPU-bound work on the default thread
        executor is a known weak spot; switch to{' '}
        <code className="text-fd-foreground">executor=&quot;process&quot;</code> and it ties. Full
        methodology and raw numbers in the{' '}
        <Link href="/docs/comparison-report" className="underline text-fd-foreground">
          comparison report
        </Link>
        .
      </p>
    </div>
  );
}
