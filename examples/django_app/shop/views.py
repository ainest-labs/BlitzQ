import uuid

from demo.blitzq_app import app
from django.db import transaction
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from blitzq.integrations.django import enqueue_on_commit

from .tasks import send_receipt


@csrf_exempt
def create_order(request):
    with transaction.atomic():
        order_id = uuid.uuid4().hex  # e.g. Order.objects.create(...).pk
        # Published only if and when this transaction commits.
        task_id = enqueue_on_commit(send_receipt, order_id)
    return JsonResponse({"order_id": order_id, "task_id": task_id}, status=202)


def task_state(request, task_id):
    state = app.status_sync(task_id)  # add authorization in real applications
    return JsonResponse({"task_id": task_id, "state": state.value if state else None})
