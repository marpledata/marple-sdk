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
- `.gitlab-ci.yml`: GitLab CI for Python tests, lint, typing, Rust tests, manual
  release builds, and docs publishing.

## Common Commands

- Python tests: `cd python && uv run pytest -v`
- Python lint/format/types: `cd python && uv run isort src tests --check --diff && uv run flake8 --config .flake8 src tests && uv run black --check src tests && uv run mypy --install-types --non-interactive`
- Python docs: `cd python && uv run --group docs sphinx-build -b html docs docs/_build/html`
- Rust workspace tests: `cd rust && cargo test --workspace --locked`
- Rust workspace lint/format/docs: `cd rust && cargo fmt --all -- --check && cargo clippy --workspace --locked --all-targets -- -D warnings && RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`
- Rust workspace examples: `cd rust && cargo build --workspace --examples --locked`
- Parquet transcoder tests: `cd rust/parquet-transcode && cargo test`

## Conventions

- Integration tests may need `MDB_TOKEN`, `MDB_URL`, and for Python Insight
  flows `INSIGHT_TOKEN`. Tests should skip gracefully when credentials are
  absent.
- Prefer small, scoped changes that match the package boundary you are working
  in.
- **Newspaper rule**: Modules read like a newspaper — public API and high-level
  flow at the top, private helpers and details below. Keep callers above callees;
  do not interleave `_helpers` between public functions.

## Releases

Short procedures:

- Python: `python/RELEASING.md` (TestPyPI → PyPI → GitLab `pages`)
- Rust: `rust/RELEASING.md` (`marple-db` → `mdb-cli` → tag `mdb-cli-v*` + binaries)
- MATLAB: `matlab/RELEASING.md` (tag `matlab-v*`)

GitHub tags/releases: `mdb-cli-v*`, `matlab-v*`, and `parquet-transcode-v*` when
that binary changes. `marpledata` and `marple-db` ship only to PyPI / crates.io;
their CHANGELOGs are the history. CLI footnote links use `mdb-cli-v*`, not bare
`v*`.
