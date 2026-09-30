"""FastAPI publishes tasks; a separate worker process executes them.

    pip install fastapi uvicorn
    uvicorn examples.fastapi_app:app          # web process
    blitzq worker examples.fastapi_app:queue  # worker process

    curl -X POST localhost:8000/orders/ORD-1
    curl localhost:8000/tasks/<task_id>

The worker imports this module too, but only uses ``queue`` and the tasks; it
never runs the web app and never sees request objects.
"""

from fastapi import FastAPI

from blitzq import Queue, TaskFailed
from blitzq.integrations.asgi import lifespan

queue = Queue("default", "redis://localhost:6379/0")


@queue.task(retries=3)
async def process_order(order_id: str) -> dict:
    # Pass identifiers, not ORM objects or request data: load what you need here.
    return {"order_id": order_id, "processed": True}


app = FastAPI(lifespan=lifespan(queue))  # connects at startup, closes at shutdown


@app.post("/orders/{order_id}", status_code=202)
async def create_order(order_id: str) -> dict:
    handle = await process_order.enqueue(order_id)
    return {"task_id": handle.id}


@app.get("/tasks/{task_id}")
async def task_status(task_id: str) -> dict:
    # Add your own authorization here: task results may contain sensitive data.
    info = await queue.inspect(task_id)
    if info is None:
        # Without track_state=True, queued and running tasks have no record yet
        # (and records expire after result_ttl).
        return {"id": task_id, "state": "pending"}
    body: dict = {"id": info.id, "state": info.state.value}
    if info.state.is_final:
        try:
            body["result"] = await queue.get_result(task_id, timeout=0)
        except TaskFailed as exc:
            body["error"] = exc.error_type
    return body
