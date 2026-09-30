# Benchmark image: runs BlitzQ, Celery and Huey workers on Linux so all
# systems use their supported process models (Celery's prefork pool is not
# supported on Windows). The repository is bind-mounted at /app, so source
# changes do not require a rebuild; only dependency changes do.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app/src:/app
WORKDIR /app

RUN pip install --no-cache-dir \
      "msgspec==0.22.0" "typer==0.27.2" \
      "celery[redis]==5.6.3" "gevent>=24.2" \
      "huey==3.4.0" \
      "pyyaml>=6" "psutil>=5.9" "matplotlib>=3.8"
