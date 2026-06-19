# Changelog

All notable changes to the Python SDK package `marpledata` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [3.2.4] - 2026-06-19

### Added

- Added `DataStream.add_dataset`, `Dataset.upsert_signals`, `Dataset.append`, and `Dataset.cool` for realtime ingest.
- Extended `Dataset.wait_for_import` to recognize `COOLING` as a busy status.

### Changed

- `DB.add_dataset`, `DB.upsert_signals`, and `DB.dataset_append` now delegate to the DataStream and Dataset methods.
- Fixed broken `DB.download_signal` compatibility path (`get_parquet_files` → `list_parquet_files`).

## [3.2.3] - 2026-06-08

### Changed

- Bugfix build artifact `insight.py`

## [3.2.2] - 2026-06-01

### Changed

- Added default request timeouts and bounded retries to improve SDK reliability on spotty networks.
- Retried idempotent direct-storage uploads while keeping Marple API POST retries conservative.

## [3.2.1] - 2026-05-22

### Changed

- Included the SDK version in the `X-Request-Source` header sent by the Marple Insight client, matching the Marple DB client.

## [3.2.0] - 2026-05-22

### Added

- Added the current Marple DB ingestion flow to `DataStream.push_file`.
- Added automatic upload mode handling for server upload, Azure direct upload, single presigned upload, and multipart upload.
- Added `concurrency` to control parallel direct-storage uploads.
- Added `upload_mode="server"` to force uploads through the Marple DB API server when direct storage URLs are blocked.
- Added `azure-storage-blob` as a runtime dependency for Azure-backed direct uploads.

### Changed

- Updated upload error handling to abort failed ingestions with a reason.
- Undeprecated `DB.delete_stream` and `DB.delete_dataset` convenience methods.
- Expanded README and Sphinx documentation for large-file uploads, forced server uploads, and integration test configuration.

### Notes

- `DB.push_file`, `DB.download_signal`, and `DB.update_metadata` remain deprecated compatibility paths. Prefer `DataStream`, `Dataset`, and `Signal` methods for new code.
