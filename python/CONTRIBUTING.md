## Development (Python)

### Formatting, linting, and typing

Checks run on `src/` and `tests/` in GitLab CI (`python:lint`, `python:typing`).

Fix formatting locally:

```bash
uv run isort src tests
uv run black src tests
```

Verify (mirror CI):

```bash
uv run isort src tests --check --diff
uv run flake8 --config .flake8 src tests
uv run black --check src tests
uv run mypy --install-types --non-interactive
```

### Testing

The testing suite runs against a real linked DB & Insight deployment on our SaaS (e.g. Castro Comrades).
Among other things it will create a stream, ingest a dataset, and export that dataset from Insight
Upload tests also exercise server and multipart upload flows, and create/delete temporary streams with a test prefix.

```bash
export MDB_TOKEN=...
export INSIGHT_TOKEN=...
# Optional, defaults to SaaS URLs:
export MDB_URL=https://db.marpledata.com/api/v1
export INSIGHT_URL=https://insight.marpledata.com/api/v1
uv run pytest -vs
```

### Documentation

Build the Sphinx docs locally:

```bash
uv sync --group docs
uv run sphinx-build -b html docs docs/_build/html
```

Open `docs/_build/html/index.html` in your browser to view the site.

### Local build

- `uv build`
- `uv run pip install dist/*.whl` (Install in your local .venv)
- `uv run python`
  - `import marple`
  - `marple.__version__`
  - `from marple import Insight, DB`

### Publishing

See [`RELEASING.md`](RELEASING.md).
