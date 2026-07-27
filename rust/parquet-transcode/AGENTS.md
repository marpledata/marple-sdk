# Parquet Transcoder Guide

Standalone Rust crate for MATLAB Marple DB compatibility. Excluded from the
parent Rust workspace.

## Structure

- `Cargo.toml` / `Cargo.lock`: standalone package.
- `src/main.rs`: CLI dispatch.
- `src/transcode.rs`: legacy directory ZSTD → Snappy rewrite.
- `src/prepare_upload.rs`: staging → Iceberg lake upload conversion.
- `tests/transcode_test.rs`: download-path regression tests.
- `tests/prepare_upload_test.rs`: prepare-upload smoke tests.
- `tests/oracle_prepare_upload.py`: independent PyArrow 24 oracle.
- `test_data/`: lake Parquet fixtures (directory names are labels).
- `README.md`: usage and release binary naming.
- `analyze.py`: ad-hoc Parquet inspection helper.

## Commands

Run from `rust/parquet-transcode/`:

- Build: `cargo build`
- Release binary: `cargo build --release`
- Rust tests: `cargo test --locked`
- Oracle: `PARQUET_TRANSCODE_BIN=target/debug/parquet-transcode uv run --with 'pyarrow==24.0.0' tests/oracle_prepare_upload.py`
- Manual directory mode: `cargo run -- <directory>`
- Manual upload mode: `cargo run -- prepare-upload --input ... --output ... --dataset-id ... --signal-id ...`

## Conventions

- Keep the legacy `parquet-transcode <directory>` invocation unchanged;
  `matlab/DB.m` depends on it.
- `prepare-upload` owns the authoritative lake schema (field IDs 1–5,
  ZSTD, 1_048_576-row groups). Do not make those settings optional.
- Prefer semantic fixture checks over byte-identical Parquet goldens.
- `test_data/dataset=8` provides numeric and text enum sources; staging
  files are derived temporarily by tests/oracle, not committed.
- Do not depend on the parent `rust/` workspace for build or test
  commands.
- Keep release binary naming aligned with `README.md` and `matlab/DB.m`.
