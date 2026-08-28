# Rust SDK Guide

This crate is the async Rust SDK for the MarpleDB API. The package name is
`marple-db`; Rust code imports it as `marple_db`.

## Structure

- `src/lib.rs`: public crate documentation and exports.
- `src/client.rs`: `MarpleDB` client and API helpers.
- `src/models.rs`: public response and option types.
- `src/errors.rs`: SDK error type and result alias.
- `src/retry.rs`: HTTP timeout/retry policy matching the Python SDK.
- `src/upload.rs`: upload mode negotiation, `UploadSession`, and upload implementations.
- `src/progress.rs`: progress reporting traits and no-op reporter.
- `tests/unit.rs`: serde and public-model unit tests.
- `tests/http.rs`: mock HTTP tests for retries, `patch`, and downloads.
- `tests/upload.rs`: upload state-machine tests against a mock HTTP API.
- `tests/integration.rs`: live API integration tests.
- `examples/push_file.rs`: minimal upload example.

## Commands

- Test this crate from `rust/`: `cargo test -p marple-db --locked`
- Run all workspace tests from `rust/`: `cargo test --workspace --locked`
- Lint/format/docs from `rust/`: `cargo fmt --all -- --check && cargo clippy --workspace --locked --all-targets --all-features -- -D warnings && RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`
- Build examples from `rust/`: `cargo build -p marple-db --examples --locked`

## Conventions

- Keep public API changes deliberate. Types exported from `lib.rs` are consumer
  facing.
- Prefer typed helpers on `MarpleDB` for stable API behavior and generic
  `get`, `post`, `patch`, and `delete` helpers for endpoints that do not have typed
  wrappers yet. Public response types should keep deserializing when the API
  adds fields or enum values (`#[serde(default)]`, `#[serde(other)]`, `extra`).
- Use `marple_db::Error` for SDK errors. Do not expose `reqwest` types on
  everyday APIs (`status` is `u16`; transport causes are `SourceError`).
  `MarpleDBBuilder::client` / `storage_client` take `reqwest::Client` and
  follow reqwest's semver.
- The SDK is async on Tokio and does not install a runtime. Examples may use
  `#[tokio::main]`. Default TLS is `rustls-tls-native-roots` (OS certificate
  store). Optional `rustls-tls` (Mozilla CAs) and `native-tls`.
- Upload changes should consider all upload modes: server, single direct
  storage, multipart, and Azure block upload.
