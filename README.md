# l2-pipeline

Multi-exchange L2 order book ingestion and reconstruction pipeline. Connects to
Binance (`@depth`) and OKX (`books`) diff-depth websocket feeds, reconstructs a
consistent in-memory order book per (exchange, symbol) following each
exchange's official synchronization protocol, and persists normalized
snapshots to Parquet with latency/throughput metrics exposed via Prometheus.

See [DECISIONS.md](DECISIONS.md) for the rationale behind every non-obvious
architectural choice.

## Requirements

- Python 3.12 (pinned — see DECISIONS.md). If your system Python is a
  different version, install 3.12 separately (e.g. via the `py` launcher on
  Windows, or [pyenv](https://github.com/pyenv/pyenv) on macOS/Linux) and
  point `uv` at it: `uv venv --python 3.12`.
- [uv](https://docs.astral.sh/uv/) for dependency management.

## Setup

```bash
uv venv --python 3.12
uv sync
```

This installs runtime and dev dependencies into `.venv` per `pyproject.toml`
and `uv.lock`.

## Running

```bash
uv run python -m l2_pipeline.app --config config/default.yaml
```

## Development

```bash
uv run pytest       # tests
uv run mypy          # type checking (strict mode)
uv run ruff check .  # linting
```

CI (`.github/workflows/ci.yml`) runs all three on every push/PR.

## Project layout

```
src/l2_pipeline/
  config.py       # config schema (dataclasses) + YAML loader
  logging_setup.py# structured JSON logging
  app.py          # entrypoint
  book/           # pure order book engine (no I/O) — M1
  feeds/          # per-exchange WS + REST clients — M3/M4
  sinks/          # Parquet writer, Prometheus metrics — M5/M6
tests/
  unit/           # book engine + config unit tests
  synthetic/       # fault-injection harness — M2
```

## Status

Work-in-progress portfolio project, built milestone by milestone. Currently:
M0 (project skeleton, config, logging, CI).
