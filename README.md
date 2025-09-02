# Marple SDK

Powerful SDKs for [Marple](https://www.marpledata.com) products.

All documentation is available on [docs.marpledata.com/sdk](https://docs.marpledata.com/docs/marple-db/api-and-sdk).

## Development (MATLAB)

You can use [MATLAB online](https://matlab.mathworks.com/) for testing.
It has a free tier for 20h of MATLAB / month.

## Development (Python)

**Local testing**

There are two small tests scripts, you do need to add your own API token in the scripts:

- `PYTHONPATH=src poetry run python tests/test_db.py`
- `PYTHONPATH=src poetry run python tests/test_insight.py`

**Local build**

- `poetry build`
- `poetry shell `
- (inside shell) `pip install dist/*.whl`
- (inside shell) `python`
- (inside repl) `from marple import Marple, Insight, DB`

**Publishing (Test Pypi)**

Only once on your laptop

- `poetry config repositories.testpypi https://test.pypi.org/legacy/`
- `poetry config http-basic.testpypi __token__ pypi-XXXXXXXXXXXXXXXXXXXXXXXXXXXX` (see 1Password)

For every build

- Bump the version number in `pyproject.toml`
- `poetry build`
- `poetry publish -r testpypi --build`

To install the test pypi package

`pip install --upgrade -i https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple marpledata` (e.g. inside `python:latest` docker container)

**Publishing (real Pypi)**

⚠ **Impacts users, be careful**

- `poetry config pypi-token.pypi pypi-XXXXXXXXXXXXXXXXXXXXXXXXXXXX`
- `poetry build`
- `poetry publish --build`
