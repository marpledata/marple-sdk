# Releasing `marpledata`

Bump `pyproject.toml`, `__version__`, and `CHANGELOG.md` on `main` first.
Tokens: 1Password (`username: __token__`). Run from `python/`.

1. **TestPyPI** — `rm -rf dist && uv build && uv publish --index testpypi`  
   Smoke: `pip install -i https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple marpledata==<version>`
2. **PyPI** — `rm -rf dist && uv build && uv publish`
3. **Docs** — run the GitLab `pages` job (Sphinx → https://marpledata.gitlab.io/marple-sdk/)

No GitHub tag/release; `CHANGELOG.md` is the release history.
