"""Application used by CLI subprocess tests."""

import os

from blitzq import Every, Queue

app = Queue(redis_url=os.environ["BQ_URL"], namespace=os.environ["BQ_NS"], mode="fast")


@app.periodic(Every(0.5), name="cli.tick")
async def tick() -> str:
    return "tick"


@app.task(name="cli.fail")
async def fail() -> None:
    raise ValueError("always fails")


@app.task(name="cli.echo")
async def echo(x: int) -> int:
    return x
