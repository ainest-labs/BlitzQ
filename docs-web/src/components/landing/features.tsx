const features = [
  {
    title: 'Async-native workers',
    description:
      'Thousands of concurrent async def tasks per process; sync functions on a bounded thread pool; CPU-bound functions on a process pool.',
  },
  {
    title: 'Two explicit delivery modes',
    description:
      'reliable (Redis Streams, at-least-once, crash recovery) and fast (Redis lists, at-most-once, fewest round-trips) — you choose per queue.',
  },
  {
    title: 'Batteries included',
    description:
      'Retries with backoff and jitter, dead letters, delayed tasks, cron/interval periodic tasks, timeouts, cancellation, results, multi-queue routing, per-queue concurrency, priority, rate limiting.',
  },
  {
    title: 'Framework-agnostic',
    description:
      'A plain Python core with optional FastAPI/Starlette, Django and Flask helpers — use it from any stack.',
  },
];

export function Features() {
  return (
    <div className="not-prose grid gap-4 sm:grid-cols-2">
      {features.map((f) => (
        <div key={f.title} className="rounded-xl border border-fd-border bg-fd-card p-5">
          <h3 className="font-semibold mb-1.5">{f.title}</h3>
          <p className="text-sm text-fd-muted-foreground">{f.description}</p>
        </div>
      ))}
    </div>
  );
}
