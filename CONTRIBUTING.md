# Contributing

## Development setup

```bash
python -m venv .venv && . .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
docker compose up -d redis                        # Redis for integration tests
```

## Checks (all must pass; CI runs the same)

```bash
ruff format src tests benchmarks examples
ruff check src tests benchmarks examples
mypy                                  # strict, over src/blitzq
pytest -q                             # unit + integration + reliability
pytest -q tests/unit                  # no external services needed
```

Integration and reliability tests use a real Redis at
`BLITZQ_TEST_REDIS_URL` (default `redis://localhost:6379/15`). They are skipped
when Redis is unreachable unless `BLITZQ_REQUIRE_REDIS=1` is set, which is what CI
does. Every test uses its own random namespace and cleans it up.

Test layout:

- `tests/unit`: pure logic and worker semantics on the in-memory broker.
- `tests/integration`: Redis, both modes (`mode` fixture), CLI, scheduler, frameworks.
- `tests/reliability`: worker crashes (real subprocess kills), Redis connection
  loss, ack failures, poison messages, lease loss.

Timing-sensitive tests assert ordering and generous bounds, never exact
milliseconds.

## Design rules

- Redis commands live only in `src/blitzq/broker/`. The worker, client and
  scheduler talk to the `Broker` interface.
- Never deserialize broker data with pickle or anything that can execute code.
- Performance changes need a benchmark result (before and after) in the PR
  description.
- Don't claim guarantees the code does not provide. Update
  `docs/delivery_guarantees.md` with any semantic change.

## Benchmarks

See [docs/benchmarking.md](docs/benchmarking.md).

## Releasing

1. Update `src/blitzq/_version.py` (semantic versioning) and `CHANGELOG.md`.
2. `python -m build && twine check --strict dist/*` and install the wheel in a
   clean virtualenv to smoke-test it.
3. Commit, then tag `vX.Y.Z` and push the tag. `.github/workflows/release.yml`
   rebuilds, checks that the tag matches the package version, and publishes to
   PyPI through trusted publishing (configure the `pypi` environment and the
   trusted publisher on pypi.org once).
4. Verify with `pip install blitzq==X.Y.Z` in a clean environment. Publication is
   done only once that install succeeds.
