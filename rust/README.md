# Marple Rust SDK and CLI

This directory contains the Rust crates for Marple DB.

## Crates

- `mdb-sdk/`: async Rust SDK published as `marple-db` and imported as `marple_db`.
- `mdb-cli/`: command-line client published as `mdb-cli`; it installs the `mdb` binary.
- `parquet-transcode/`: standalone helper binary used by the MATLAB SDK for older MATLAB Parquet compatibility. This crate is excluded from the Rust workspace.

The workspace in this directory contains only `mdb-sdk` and `mdb-cli`.

## Common Commands

Run these from `rust/`:

```sh
cargo test --workspace --locked
cargo build --workspace --examples --locked
cargo test -p marple-db --locked
cargo test -p mdb-cli --locked
cargo run -p mdb-cli -- --help
```

Run these from `rust/parquet-transcode/`:

```sh
cargo test
cargo build --release
```

## Authentication

Integration tests and local examples may need Marple DB credentials:

```sh
export MDB_TOKEN="mdb_your_token_here"
export MDB_URL="https://db.marpledata.com/api/v1"
```

`MDB_URL` is optional for SaaS usage and should usually end in `/api/v1` for VPC or self-hosted deployments.

## Publishing

For release steps, see `RELEASING.md`.

Package release notes live in:

- `mdb-sdk/CHANGELOG.md`
- `mdb-cli/CHANGELOG.md`
