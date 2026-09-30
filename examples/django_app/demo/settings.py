"""Minimal Django settings for the BlitzQ example."""

SECRET_KEY = "example-only-not-secret"
DEBUG = True
ALLOWED_HOSTS = ["*"]
INSTALLED_APPS = ["shop"]
ROOT_URLCONF = "demo.urls"
DATABASES = {"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": "example.sqlite3"}}
USE_TZ = True
BLITZQ_REDIS_URL = "redis://localhost:6379/0"
