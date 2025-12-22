## Development (Python)

### Testing

The testing suite runs against a real linked DB & Insight deployment on our SaaS (e.g. Castro Comrades).
Among other things it will create a stream, ingest a dataset, and export that dataset from Insight

```bash
export MDB_TOKEN=...
export INSIGHT_TOKEN=...
uv run pytest -vs
```

### Local build

- `uv build`
- `uv run pip install dist/*.whl` (Install in your local .venv)
- `uv run python`
  - `import marple`
  - `marple.__version__`
  - `from marple import Marple, Insight, DB`

### Publishing (Test Pypi)

Only once on your laptop

- `uv build`
- `uv publish --index testpypi`
  - `username: __token__`
  - `password: pypi-XXXXXXXXXXXXXXXXXXXXXXXXXXXX` (see 1Password)

For every build

- `uv version x.y.z.devi`
- bump version in `__version__` variable
- `uv build`
- `uv publish --index testpypi`

**Versioning**

To ensure pip correctly updates our package, correct versioning is important. Use `major.minor.patch`

- `major` when they make incompatible API changes
- `minor` when they add functionality in a backwards-compatible manner
- `patch` when they make backwards-compatible bug fixes

**Recommended test workflow**

- Publish to test Pypi
- Open a docker container with the desired python version: `docker run -it python:3.11-alpine sh`
- Install the test pypi package `pip install --upgrade -i --pre https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple marpledata`
- If you want to publish again, change the version to one that has not been used before. Even if you delete a build via [the UI](https://test.pypi.org/manage/project/marpledata/releases/) you cannot publish that version again. For testing, you could use something like `x.y.z.dev1`, `x.y.z.dev2`, `x.y.z.dev3`, ...

### Publishing (real Pypi)

⚠ **Impacts users, be careful**

- check version: `pyproject.toml:version`, `__init__.py:__version__`
- `uv config pypi-token.pypi pypi-XXXXXXXXXXXXXXXXXXXXXXXXXXXX`
- `uv build`
- `uv publish`
