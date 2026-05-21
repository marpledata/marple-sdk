# Parquet Transcoder Guide

This is a standalone Rust crate for transcoding ZSTD-compressed Parquet files to
Snappy for older MATLAB versions. It is excluded from the parent Rust workspace.

## Structure

- `Cargo.toml`: standalone crate manifest.
- `Cargo.lock`: standalone lockfile.
- `src/main.rs`: CLI implementation.
- `tests/transcode_test.rs`: integration tests for transcoding behavior.
- `test_data/`: crate-local Parquet fixtures.
- `README.md`: usage and release binary naming.
- `analyze.py`: helper script for inspecting Parquet files.

## Commands

- Build: `cargo build`
- Build release binary: `cargo build --release`
- Run tests: `cargo test`
- Run manually: `cargo run -- <directory>`

Run commands from `rust/parquet-transcode/`.

## Conventions

- Preserve in-place transcoding behavior: the command recursively walks a
  directory and rewrites only Parquet files that need compatible compression.
- Keep tests local to `tests/` and use fixtures from `test_data/`.
- Do not depend on the parent `rust/` workspace for build or test commands.
- Keep release binary naming aligned with `README.md` and the MATLAB downloader
  in `matlab/DB.m`.
- Be careful with fixture churn. Parquet binary fixture changes should be
  intentional and explained.
- This tool exists for MATLAB compatibility; avoid changes that require newer
  MATLAB features in the consuming client.
