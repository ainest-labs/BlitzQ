"""Producer subprocess: enqueues a share of a workload and reports timings as JSON.

Run by the runner with the system's environment already applied, so each
system's application module is imported fresh with the right configuration.
"""

from __future__ import annotations

import json
import random
import sys
import time

from .adapters import get_adapter
from .workloads import Workload


def items_for(w: Workload, start: int, count: int, seed: int) -> list[tuple[int, str, float]]:
    rng = random.Random(seed)
    names = list(w.queues)
    weights = [w.queues[n] for n in names]
    out = []
    for uid in range(start, start + count):
        q = names[0] if len(names) == 1 else rng.choices(names, weights)[0]
        delay = w.delay_s if w.kind == "scheduled" else 0.0
        out.append((uid, q, delay))
    return out


def main() -> None:
    spec = json.loads(sys.argv[1])
    w = Workload.from_dict(spec["workload"])
    adapter = get_adapter(spec["system"], spec["settings"], w, spec["broker_url"])
    items = items_for(w, spec["start"], spec["count"], spec["seed"])
    producer = adapter.make_producer()
    params = w.params()
    clock = time.perf_counter
    t0 = clock()
    producer.produce(items, clock, params)
    t1 = clock()
    producer.close()
    print(json.dumps({"t_start": t0, "t_end": t1, "count": len(items)}))


if __name__ == "__main__":
    main()
