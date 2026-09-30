import Link from 'next/link';
import { Features } from '@/components/landing/features';
import { LiveDemo } from '@/components/landing/live-demo';
import { BenchmarkChart } from '@/components/landing/benchmark-chart';

export default function HomePage() {
  return (
    <main className="flex flex-1 flex-col">
      <section className="flex flex-col items-center text-center gap-5 px-4 pt-20 pb-16">
        <span className="rounded-full border border-fd-border px-3 py-1 text-xs font-medium text-fd-muted-foreground">
          1.0.0 · <code className="text-fd-foreground">pip install blitzq</code>
        </span>
        <h1 className="text-4xl sm:text-5xl font-bold tracking-tight max-w-2xl">
          A fast, async-native task queue for Python
        </h1>
        <p className="text-fd-muted-foreground max-w-xl text-lg">
          BlitzQ runs background jobs on <code>asyncio</code> and Redis, with explicit
          reliable/fast delivery modes, retries, scheduling and multi-queue routing —
          framework-agnostic, with FastAPI, Django and Flask helpers.
        </p>
        <div className="flex flex-wrap items-center justify-center gap-3 mt-2">
          <Link
            href="/docs"
            className="rounded-full bg-fd-primary text-fd-primary-foreground px-5 py-2.5 text-sm font-semibold"
          >
            Read the docs
          </Link>
          <Link
            href="/docs/installation"
            className="rounded-full border border-fd-border px-5 py-2.5 text-sm font-semibold"
          >
            Installation
          </Link>
          <a
            href="https://github.com/ainest-labs/BlitzQ"
            className="rounded-full border border-fd-border px-5 py-2.5 text-sm font-semibold"
          >
            GitHub
          </a>
        </div>

        <pre className="mt-8 w-full max-w-xl rounded-xl border border-fd-border bg-fd-card text-left text-xs sm:text-sm p-4 overflow-x-auto">
          <code>{`from blitzq import Queue

queue = Queue(name="default", redis_url="redis://localhost:6379/0")

@queue.task(retries=3)
async def process_order(order_id: str) -> dict:
    return {"order_id": order_id, "processed": True}

task = await process_order.enqueue("ORD-123")
result = await queue.get_result(task.id, timeout=10)`}</code>
        </pre>
      </section>

      <section className="px-4 pb-16 max-w-3xl mx-auto w-full">
        <Features />
      </section>

      <section className="px-4 pb-16 max-w-3xl mx-auto w-full">
        <div className="mb-6 text-center">
          <h2 className="text-2xl font-bold mb-2">Race BlitzQ against Celery</h2>
          <p className="text-fd-muted-foreground text-sm">
            Pick a workload and a task count, then watch both clear the queue at their
            real measured throughput.
          </p>
        </div>
        <LiveDemo />
      </section>

      <section className="px-4 pb-24 max-w-3xl mx-auto w-full">
        <div className="mb-6 text-center">
          <h2 className="text-2xl font-bold mb-2">Benchmarked against Celery and Huey</h2>
          <p className="text-fd-muted-foreground text-sm">
            Measured, not estimated — 5 repetitions per workload, medians reported.
            BlitzQ wins most workloads, and the report says so when it doesn&apos;t.
          </p>
        </div>
        <BenchmarkChart />
      </section>
    </main>
  );
}
