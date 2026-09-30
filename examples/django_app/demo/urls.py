from django.urls import path
from shop import views

urlpatterns = [path("orders/", views.create_order), path("tasks/<str:task_id>/", views.task_state)]
