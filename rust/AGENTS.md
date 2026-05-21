# Rust Workspace Guide

This directory contains the Rust SDK, CLI, and parquet transcoder.

## Structure

- `Cargo.toml`: workspace manifest.
- `mdb-sdk/`: Rust SDK crate published as `marple-db` and imported as
  `marple_db`.
- `mdb-cli/`: CLI crate whose binary is `mdb`.
- `parquet-transcode/`: standalone crate excluded from the workspace. It has its
  own manifest, lockfile, tests, and fixtures.

## Commands

- Test workspace crates: `cargo test --workspace --locked`
- Build workspace examples: `cargo build --workspace --examples --locked`
- Test SDK only: `cargo test -p marple-db --locked`
- Test CLI only: `cargo test -p mdb-cli --locked`
- Test parquet transcoder: `cd parquet-transcode && cargo test`

Run workspace commands from `rust/`. Run parquet transcoder commands from
`rust/parquet-transcode/` because it is excluded from the workspace.

## Conventions

- Keep SDK behavior in `mdb-sdk` and CLI behavior in `mdb-cli`.
- `parquet-transcode` is completely seperate from the Marple DB sdk
- Use shared CSV fixtures from `../test_data/` when tests need repo-wide sample
  data.
- Integration tests may need `MDB_TOKEN` and optionally `MDB_URL`. Tests should
  skip gracefully when credentials are absent.
- Keep Cargo dependency versions centralized in `[workspace.dependencies]` when
  the dependency is shared by workspace crates.
- Preserve the Rust 2024 edition and the existing async/Tokio patterns.
