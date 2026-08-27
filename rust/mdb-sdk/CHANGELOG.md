# Changelog

All notable changes to `marple-db` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Typed helpers for license, user/workspace info, usage, settings, stream metadata, `get_stream_by_id()`, and `get_signals()`.
- `download_original()` / `download_original_with_progress()`.
- TLS features (`rustls-tls-native-roots` default, `rustls-tls`, `native-tls`), `SAAS_URL`, `VERSION`, and a generic `patch` helper.
- `PushFileOptions::dataset_name`, `StorageQuota`, `Settings`, and `ImportStatus` helpers (`as_str`, `is_success`, `is_failure`).

### Changed

- IDs are `i64`. The crate requires a Tokio runtime. HTTP errors no longer expose `reqwest` types. `storage_client()` was removed in favor of `download_original`. Custom TLS goes through `MarpleDBBuilder::client` / `storage_client`.
- Timeouts and retries match the Python SDK. `PushFileOptions` uses chained setters. MSRV is Rust 1.85; crates.io links to docs.rs.
- `wait_for_import` treats `CoolingFailed` as failure. Oversized upload chunks are `Error::Protocol`; `Error::IntegerConversion` was removed.

### Fixed

- Parse JSON numbers outside `i64` (e.g. signal stats) and `/user/info` `last_active` floats/`null`.
- `push_file` returns a configuration error when the local path has no file name, instead of panicking.

## [0.3.0] - 2026-08-20

### Added

- `PushFileOptions` / `PushFileOptionsBuilder::overwrite` to replace an existing dataset with the same name on ingest.
- `ImportStatus::Cooling` and `ImportStatus::CoolingFailed` for realtime dataset cooling lifecycle.

## [0.2.1] - 2026-05-22

### Added

- Sent an `X-Request-Source: sdk/rust:<version>` header on every API request so SDK traffic shows up in backend logs and metrics alongside the Python and MATLAB SDKs. Storage-client requests against pre-signed URLs are unaffected.
- Added `MarpleDBBuilder::request_source` so higher-level tools built on the SDK can override the default `X-Request-Source` value and identify themselves distinctly (for example `cli/rust:<version>` from `mdb-cli`).

## [0.2.0] - 2026-05-22

### Added

- Initial public release of the async Rust SDK for the MarpleDB API.
- Added the `MarpleDB` client with typed helpers for health checks, streams, datasets, datapool datasets, and ingest queue listing.
- Added generic `get`, `post`, and `delete` helpers for API endpoints that do not have typed SDK wrappers yet.
- Added file ingestion through the current Marple DB ingestion API, including server upload, single direct storage upload, multipart upload, and Azure block upload modes.
- Added `PushFileOptions` with metadata, file naming, upload concurrency, upload mode override, and progress reporting support.
- Added original-file download link support through pre-signed storage URLs.
- Added structured SDK errors with API and storage status/body context.
- Added crate documentation, examples, integration tests, and unit tests for public models and upload behavior.

### Changed

- Standardized the crate license metadata and packaged license file on Apache-2.0.
