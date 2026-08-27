# Changelog

All notable changes to `marple-db` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `SAAS_URL` for the hosted Marple DB API root, `VERSION` for the crate version, and a generic `patch` helper alongside `get` / `post` / `delete`.
- `download_original()` / `download_original_with_progress()` to fetch a dataset's original uploaded file into a directory.
- TLS Cargo features: `rustls-tls-native-roots` (default; OS certificate store), `rustls-tls` (Mozilla CAs only), and `native-tls`.
- Typed helpers for `/workspace/license`, `/user/info`, `/usage/series/{usage_type}`, `/settings`, and `/stream/{id}/metadata/fields`, plus `get_current_workspace()` to resolve the connected workspace from `/user/info` (name, id, license quotas, and latest storage usage).
- `get_stream_by_id()` for `GET /stream/{id}`. `create_stream` and `update_stream` now reload through that endpoint instead of re-listing every stream.
- `StorageQuota` for license byte caps (`limit < 0` is unlimited). Missing license/signal/dataset/stream fields default; unknown enum values become `Unknown`.
- `get_signals()` and a typed `Signal` model for dataset signal metadata.
- `Settings` for `/settings`, with `INSIGHT_URL` and other known keys typed and remaining keys in `extra`.
- `ImportStatus::as_str`, `is_success`, `is_failure`, and `Display` using API names such as `FINISHED`.
- `PushFileOptions::dataset_name` to set the ingested dataset path independently of the local file name.

### Changed

- Stream, dataset, signal, user, license, and ingestion ids are `i64` so they match JSON integers and cannot overflow an `i32`.
- `wait_for_import` treats `CoolingFailed` as a terminal import failure, polls with `tokio::time::timeout`, and reports API status names on timeout.
- `PushFileOptions` is configured with chained setters on the options value itself (`PushFileOptions::default().metadata(...).overwrite(true)`).
- SDK-built HTTP clients use the Python SDK timeout and retry defaults (5s connect / 300s API, 1800s storage; retries on the same methods and status codes).
- Documented MSRV is Rust 1.85 (edition 2024). crates.io now links to docs.rs for API docs.
- The crate requires a Tokio runtime. HTTP errors no longer expose `reqwest` types (`status` is `u16`, methods are strings, causes are `SourceError`). `storage_client()` on `MarpleDB` was removed in favor of `download_original`. Custom TLS goes through `MarpleDBBuilder::client` / `storage_client` (`reqwest::Client`; those methods follow reqwest's semver).

### Fixed

- Parse JSON numbers that do not fit in `i64` (for example signal `stats` min/max stored as `±f64::MAX` integers) instead of failing with `JSON error`.
- Parse `/user/info` workspace `last_active` values that are Postgres floats or JSON `null`, so `get_current_workspace()` no longer fails on a connected token.
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
