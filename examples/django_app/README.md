# Django example

```bash
docker compose up -d redis                     # from the repository root
cd examples/django_app
python manage.py runserver                     # terminal 1: web (publishes)
blitzq worker demo.blitzq_app:app              # terminal 2: worker (executes)
curl -X POST localhost:8000/orders/            # -> {"order_id": ..., "task_id": ...}
curl localhost:8000/tasks/<task_id>/           # -> {"state": "succeeded"}
```
