# Repository Guide

This repository is a multi-language SDK monorepo for Marple products. More
specific `AGENTS.md` files in subdirectories override or extend this guidance.

## Structure

- `python/`: Python SDK package published as `marpledata`. Source lives in
  `python/src/marple/`, tests in `python/tests/`, and Sphinx docs in
  `python/docs/`.
- `rust/`: Rust code. The workspace contains `mdb-sdk` and `mdb-cli`; the
  `parquet-transcode` crate lives under `rust/` but is excluded from the
  workspace.
- `matlab/`: MATLAB DB client, example script, and local configuration.
- `test_data/`: shared fixtures used by Python and Rust tests.
- `.gitlab-ci.yml`: GitLab CI for Python tests, Rust tests, manual release
  builds, and docs publishing.

## Common Commands

- Python tests: `cd python && uv run pytest -v`
- Python docs: `cd python && uv run --group docs sphinx-build -b html docs docs/_build/html`
- Rust workspace tests: `cd rust && cargo test --workspace --locked`
- Rust workspace examples: `cd rust && cargo build --workspace --examples --locked`
- Parquet transcoder tests: `cd rust/parquet-transcode && cargo test`

## Conventions

- Integration tests may need `MDB_TOKEN`, `MDB_URL`, and for Python Insight
  flows `INSIGHT_TOKEN`. Tests should skip gracefully when credentials are
  absent.
- Prefer small, scoped changes that match the package boundary you are working
  in.

## Releases

- Only `mdb-cli` gets GitHub releases and git tags. Tags follow the
  `mdb-cli-v<X.Y.Z>` convention (matching `parquet-transcode-v<X.Y.Z>` for the
  out-of-workspace crate).
- `marple-db` ships via `cargo publish` to crates.io and `marpledata` via
  `uv publish` to PyPI. Neither gets its own git tag or GitHub release; their
  CHANGELOGs are the canonical history.
- CHANGELOG version-footnote links should point at `releases/tag/mdb-cli-v*`,
  not `releases/tag/v*`.
