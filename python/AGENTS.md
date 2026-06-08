# Python SDK Guide

This directory contains the Python SDK package published as `marpledata`.

## Structure

- `src/marple/`: package source.
- `src/marple/db/`: Marple DB types and helpers.
- `src/marple/insight.py`: Marple Insight client.
- `tests/`: pytest integration tests.
- `docs/`: Sphinx documentation.
- `pyproject.toml`: package metadata, dependencies, and `uv` dependency groups.
- `uv.lock`: locked Python dependencies.

## Commands

- Run tests with output: `uv run pytest -vs`
- Build docs: `uv run --group docs sphinx-build -b html docs docs/_build/html`
- Build package: `uv build`
- Fix formatting: `uv run isort src tests && uv run black src tests`
- Verify lint/format/types (mirror CI):
  `uv run isort src tests --check --diff && uv run flake8 --config .flake8 src tests && uv run black --check src tests && uv run mypy --install-types --non-interactive`

Run commands from `python/` unless a command explicitly says otherwise.

## Conventions

- Use `uv` for dependency, test, build, and docs workflows.
- Keep Python SDK tests in `python/tests/`.
- Integration tests run against live Marple services and may require
  `MDB_TOKEN`, `MDB_URL`, `INSIGHT_TOKEN`, and `INSIGHT_URL`. Tests should skip
  or fail clearly when required credentials are missing.
