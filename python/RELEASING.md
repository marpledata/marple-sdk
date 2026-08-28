# Releasing `marpledata`

Tokens: 1Password (`username: __token__`). Run from `python/`.

- **Bump version**
  - `uv version --bump minor` / `uv version x.y.z`
  - Manual update `__init__.py`, `CHANGELOG.md`
- **TestPyPI** — `rm -rf dist && uv build && uv publish --index testpypi`
  Smoke: `pip install -i https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple marpledata==<version>`
- **PyPI** — `rm -rf dist && uv build && uv publish`
- **Docs** — run the GitLab `pages` job (Sphinx → https://marpledata.gitlab.io/marple-sdk/)

No GitHub tag/release; `CHANGELOG.md` is the release history.
