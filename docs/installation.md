# Installation

## Requirements

| Component | Supported | Tested |
|---|---|---|
| Python | 3.12, 3.13 | 3.12 (Windows 11 and Linux), 3.13 in CI |
| Redis server | 7.0+ (Streams consumer groups, `XAUTOCLAIM`, `LPOP`/`RPOP` count, Lua) | 7.4 |
| redis-py | 5.0+ | 6.4 (Linux benchmark image), 8.1 (Windows) |
| msgspec | 0.18+ | 0.22 |
| typer | 0.12+ | 0.27 |

Redis-compatible servers (Valkey, Dragonfly, KeyDB) have **not** been tested and
are not declared supported. Redis Cluster is not supported, because the promotion
script builds queue keys at run time.

## Install

BlitzQ is not published on PyPI yet. Install from a checkout or a built wheel:

```bash
git clone https://github.com/ainest-labs/BlitzQ.git && cd BlitzQ
pip install .                  # core
pip install ".[django]"        # + Django integration dependency
pip install ".[flask]"         # + Flask integration dependency
pip install ".[monitoring]"    # + Prometheus exporter (prometheus-client)
pip install -e ".[dev]"        # development: tests, linters, all integrations
pip install -e ".[bench]"      # benchmark suite (Celery, psutil, matplotlib)
```

Once it's released, `pip install blitzq` (with the same extras) will work. The
FastAPI/Starlette/Litestar integration needs no extra, because it imports nothing
from those frameworks.

## Redis

For local development:

```bash
docker compose up -d redis     # redis:7.4 on localhost:6379
```

Point BlitzQ at Redis with `Queue(redis_url=...)` or the `BLITZQ_REDIS_URL`
environment variable. URL forms:

- `redis://[[user]:password@]host:6379/0`
- `rediss://...` for TLS. Pass CA or client certificates via
  `Queue(redis_options={"ssl_ca_certs": "/path/ca.pem"})` (any redis-py connection
  option is accepted).
- `unix:///path/to/redis.sock?db=0`

See [operations.md](operations.md) for persistence, memory and security settings.
