# Releasing Rust Crates

This document covers publishing the Rust SDK and CLI. Run commands from `rust/` unless noted otherwise.

## Pre-Release Checks

```sh
cargo test --workspace --locked
cargo build --workspace --examples --locked
cargo package -p marple-db
```

`mdb-cli` depends on the published `marple-db` crate version. Before `marple-db` is on crates.io, `cargo package -p mdb-cli` will fail with "no matching package named `marple-db` found". That is expected during the first SDK publish or after an SDK version bump.

## Publish `marple-db`

Update and commit:

- `rust/mdb-sdk/Cargo.toml`
- `rust/Cargo.lock`
- `rust/mdb-sdk/CHANGELOG.md`
- `rust/mdb-sdk/README.md` if installation or API examples changed

Then publish:

```sh
cargo publish -p marple-db --dry-run
cargo publish -p marple-db
```

Wait for crates.io to index the new version before publishing crates that depend on it.

## Publish `mdb-cli`

Update and commit:

- `rust/mdb-cli/Cargo.toml`
- `rust/Cargo.lock`
- `rust/mdb-cli/CHANGELOG.md`
- `rust/mdb-cli/README.md` if installation or command examples changed

After the SDK version is available on crates.io:

```sh
cargo package -p mdb-cli
cargo publish -p mdb-cli --dry-run
cargo publish -p mdb-cli
```

The CLI package installs the `mdb` binary with:

```sh
cargo install mdb-cli
```

## CLI Binary Artifacts

The crates.io package is enough for `cargo install`, but users may also need downloadable binaries. Trigger the manual GitLab `build:mdb-cli` job to produce:

- `mdb-linux-x64`
- `mdb-windows-x64.exe`

Attach those artifacts to the CLI GitHub Release if you publish binary releases there.

## Parquet Transcoder

`parquet-transcode` is excluded from the workspace. Run commands from `rust/parquet-transcode/`:

```sh
cargo test
cargo package
cargo build --release
```

The manual GitLab `build:parquet-transcode` job builds release binaries consumed by the MATLAB SDK. Keep binary names aligned with `rust/parquet-transcode/README.md` and `matlab/DB.m`.
