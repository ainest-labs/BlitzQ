"""Flask publishes with the synchronous API; a separate worker executes.

pip install flask
flask --app examples.flask_app run          # web process
blitzq worker examples.flask_app:queue      # worker process

curl -X POST localhost:5000/reports -H 'content-type: application/json' -d '{"report_id": 7}'
"""

from flask import Flask, current_app, request

from blitzq import Queue
from blitzq.integrations.flask import init_app

queue = Queue("default", "redis://localhost:6379/0")
app = Flask(__name__)
app.config["REPORT_BUCKET"] = "s3://reports"
init_app(app, queue)  # sync tasks run inside app.app_context() on the worker


@queue.task
def build_report(report_id: int) -> str:
    # No request context here - only the application context.
    return f"{current_app.config['REPORT_BUCKET']}/{report_id}.pdf"


@app.post("/reports")
def create_report():  # type: ignore[no-untyped-def]
    handle = build_report.enqueue_sync(int(request.json["report_id"]))
    return {"task_id": handle.id}, 202


@app.get("/reports/<task_id>")
def report_status(task_id: str):  # type: ignore[no-untyped-def]
    state = queue.status_sync(task_id)
    return {"state": state.value if state else "unknown"}
