# Changelog

All notable changes to the Python SDK package `marpledata` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Exact signal names in `get_data` / `get_signal` / `get_signals` resolve via the dataset cache or `GET /datapool/{pool}/signal/{name}/id` instead of downloading the full datapool `signal_map`. Regex patterns still use the map.

### Added

- Processing Pipeline
  - `DB.create_script` / `get_scripts` / `get_script` / `delete_script`
  - `Script.update` / `duplicate` / `delete`, to manage stored `process(dataset)` scripts.
  - `Dataset.run` / `DB.run_script` to execute a stored processing script against a dataset via a server-side sandbox job.
  - `DataStream.scripts`, `DataStream.update`, and `DataStream.rerun_processing` (`DB.rerun_processing`) to edit a stream and set the script pipeline
  - `Dataset.rerun_processing` and `Dataset.get_debug_messages`.
- `DB.delete_signals`, `Dataset.delete_signal` / `Dataset.delete_signals`, and `Signal.delete` to remove signals from a dataset.

### Fixed

- Allow `add_signal` in post-processing

## [3.5.0] - 2026-08-20

### Added

- `Dataset.add_signal` and `Dataset.add_signals` now accept a time-indexed pandas Series or DataFrame (DatetimeIndex / TimedeltaIndex when there is no `time` column).
- `DataStream.push_file(..., overwrite=False)` to replace an existing dataset with the same name (also on the deprecated `DB.push_file`).

### Fixed

- An empty local parquet cache folder is treated as a cache miss and re-downloaded, instead of being assumed to mean the signal has no data.

## [3.4.0] - 2026-07-28

### Added

- `Dataset.add_signal` and `Dataset.add_signals` to upload signal data onto an existing dataset (presign → write → PUT → complete). Accepts DataFrame, Arrow table, or on-disk parquet matching `LAKE_ARROW_SCHEMA` (`time` plus `value` and/or `value_text`); supports concurrent uploads, batch-level `overwrite`, import `priority`, and automatic ~1M-row groups / ~16M-row file splits.
- `Signal.wait_until_available` to poll until a signal is queryable (storage status is no longer `FROZEN_TO_COLD`; typical after `add_signal`).
- `SignalUpload` input model and `SignalsAlreadyExistError` for HTTP 409 presign conflicts (duplicate / already exists).
- Exported `LAKE_ARROW_SCHEMA` describing the input columns for signal upload (alongside existing realtime `SCHEMA` for `Dataset.append`).
- `Dataset.get_signals(signal_ids=...)` to fetch signals by ID (order preserved), plus `refresh` on `get_signal` / `get_signals` to bypass the local cache.

## [3.3.0] - 2026-06-29

### Added

- `DB.query` (SQL to DataFrame) and `DB.connect_trino` (raw Trino DBAPI connection) for querying hot (Postgres metadata) and cold (Iceberg raw data) as one database via Trino. Connection details are auto-discovered; `DB.trino_info` exposes the catalog names. Not available on Marple SaaS yet.
- `trino_host` parameter on `DB` to override the derived Trino host.

## [3.2.5] - 2026-06-25

### Fixed

- Large direct-to-S3 uploads failing on slow networks with "write operation timed out".

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
