from demo.blitzq_app import app
from django.db import connection


@app.task(retries=3)
def send_receipt(order_id: str) -> str:
    # Receive an identifier, not a model instance; load data here.
    with connection.cursor() as cur:
        cur.execute("SELECT 1")
    return f"receipt sent for {order_id}"
