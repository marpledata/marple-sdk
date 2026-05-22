# Changelog

All notable changes to `marple-db` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.1] - 2026-05-22

### Added

- Sent an `X-Request-Source: sdk/rust:<version>` header on every API request so SDK traffic shows up in backend logs and metrics alongside the Python and MATLAB SDKs. Storage-client requests against pre-signed URLs are unaffected.

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
