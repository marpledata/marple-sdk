# Changelog

All notable changes to `parquet-transcode` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-07-28

Two-way Parquet helper for the MATLAB Marple DB SDK.

### Added

- **`prepare-upload` command** — converts MATLAB staging Parquet (`time` / `value` / `value_text`) into the Iceberg lake format used for Marple DB signal uploads (field IDs, ZSTD compression, `dataset` / `signal` identity columns).
- JSON stdout with upload metadata (`output`, `rows`, `size`, `footer`); diagnostics go to stderr.
- Optional `--expected-rows` validation.
- Integration tests and a Python SDK oracle for `prepare-upload`.

### Changed

- Refactored into separate download-transcode and upload-preparation modules.
- Updated to Arrow/Parquet 57.

### Unchanged

- **Directory transcode** — `parquet-transcode <directory>` still rewrites ZSTD Parquet to Snappy in place for older MATLAB versions.

## [0.1.0] - 2026-02-27

### Added

- Initial release: recursively transcode ZSTD-compressed Parquet files to Snappy for older MATLAB versions.
- Pre-built binaries for Linux x64, Windows x64, and macOS (Apple Silicon), downloaded automatically by the MATLAB SDK.

[0.2.0]: https://github.com/marpledata/marple-sdk/releases/tag/parquet-transcode-v0.2.0
[0.1.0]: https://github.com/marpledata/marple-sdk/releases/tag/parquet-transcode-v0.1.0
