"""The BlitzQ application for this Django project.

Web:     python manage.py runserver
Worker:  blitzq worker demo.blitzq_app:app      (run from examples/django_app)
"""

from blitzq import Queue
from blitzq.integrations import django as bq_django

app = Queue("default", redis_url="redis://localhost:6379/0", namespace="example-django")

# Initialises Django in worker processes, autodiscovers <app>.tasks modules and
# wraps sync tasks with close_old_connections().
bq_django.setup(app, settings_module="demo.settings")
