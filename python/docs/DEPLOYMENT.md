# Documentation Deployment

The Sphinx documentation can be manually built and deployed to GitLab Pages with a single click.

## How to Deploy
Click **Run pipeline** to trigger it
The`pages` job will start and automatically build the docs and serve them as a static gitlab page under `https://marpledata.gitlab.io/marple-sdk/`

## Local Development

To build docs locally:
```bash
cd python
uv run sphinx-build -b html docs docs/_build/html
```

Then open `docs/_build/html/index.html` in your browser.


**Sphinx configuration:** `python/docs/conf.py`
- Uses PyData theme
- Includes Pydantic filtering to hide internal methods
- 2-product structure (Insight, DB, Files)

