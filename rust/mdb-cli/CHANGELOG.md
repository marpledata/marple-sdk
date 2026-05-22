# Changelog

All notable changes to `mdb` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.1] - 2026-05-22

### Changed

- Identified CLI traffic with `X-Request-Source: cli/rust:<version>` (via the new `MarpleDBBuilder::request_source` hook in `marple-db` 0.2.1) so backend logs and metrics can distinguish CLI users from raw SDK consumers.

## [0.2.0] - 2026-05-22

### Added

- Added `--env-file` support to load credentials and settings from a dotenv file before reading `MDB_TOKEN` and `MDB_URL`.
- Added datapool dataset listing with `mdb datapool datasets`, including ingest queue output with `--queue`.
- Added short and long output formats for stream, dataset, and datapool dataset lists.
- Added `--upload-mode` and `--concurrency` ingestion controls backed by the Rust SDK upload pipeline.

### Changed

- Updated the CLI to depend on the published `marple-db` Rust SDK crate.
- Standardized the crate license metadata and packaged license file on Apache-2.0.
- Improved tabular dataset and stream output for interactive CLI usage.

## [0.1.6] - 2026-05-11

### Changed

- Updated ingestion to use the new Marple DB upload flow with `POST /ingestion`
- Added support for concurrent multipart uploads with signed part URLs

## [0.1.1] - 2026-02-19

### Fixed

- Use `stream_with_length` for file uploads to include content length in multipart requests

## [0.1.0] - 2025-12-22

### Added

- Initial release of `mdb` CLI tool
- Stream management commands (list, get, create, update)
- File ingestion with recursive directory support
- Dataset operations (list, get, download)
- Direct API access (GET, POST, DELETE endpoints)
- Health check command (`mdb ping`)
- Progress bars for file uploads and downloads
- JSON output for all structured data
- Environment variable and `.env` file support
- Cross-platform support (macOS, Linux, Windows)

[0.2.1]: https://github.com/marpledata/marple-sdk/releases/tag/v0.2.1
[0.2.0]: https://github.com/marpledata/marple-sdk/releases/tag/v0.2.0
[0.1.6]: https://github.com/marpledata/marple-sdk/releases/tag/v0.1.6
[0.1.1]: https://github.com/marpledata/marple-sdk/releases/tag/v0.1.1
[0.1.0]: https://github.com/marpledata/marple-sdk/releases/tag/v0.1.0
