# Rust SDK Guide

This crate is the async Rust SDK for the MarpleDB API. The package name is
`marple-db`; Rust code imports it as `marple_db`.

## Structure

- `src/lib.rs`: public crate documentation and exports.
- `src/client.rs`: `MarpleDB` client and API helpers.
- `src/models.rs`: public response and option types.
- `src/errors.rs`: SDK error type and result alias.
- `src/upload.rs`: upload mode negotiation and upload implementations.
- `src/progress.rs`: progress reporting traits and no-op reporter.
- `tests/test_db_sdk.rs`: SDK integration tests.
- `examples/push_file.rs`: minimal upload example.

## Commands

- Test this crate from `rust/`: `cargo test -p marple-db --locked`
- Run all workspace tests from `rust/`: `cargo test --workspace --locked`
- Build examples from `rust/`: `cargo build -p marple-db --examples --locked`

## Conventions

- Keep public API changes deliberate. Types exported from `lib.rs` are consumer
  facing.
- Prefer typed helpers on `MarpleDB` for stable API behavior and generic
  `get`, `post`, and `delete` helpers for endpoints that do not have typed
  wrappers yet.
- Use `marple_db::Error` for SDK errors and preserve useful status/body context
  for API and storage failures.
- The SDK is async but does not install a runtime. Examples may use Tokio.
- Upload changes should consider all upload modes: server, single direct
  storage, multipart, and Azure block upload.
