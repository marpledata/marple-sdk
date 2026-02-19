# Changelog

All notable changes to `mdb` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[0.1.1]: https://github.com/marpledata/marple-sdk/releases/tag/v0.1.1
[0.1.0]: https://github.com/marpledata/marple-sdk/releases/tag/v0.1.0

