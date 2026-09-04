# Changelog

All notable changes to the MATLAB DB client will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Optional `PluginArgs` on `push_file` for per-file plugin arguments. If omitted, the stream default is used.

## [0.3.1] - 2026-08-20

### Fixed

- An empty local parquet cache folder is treated as a cache miss and re-downloaded, instead of being assumed to mean the signal has no data.

### Changed

- Multipart file uploads (`push_file` server mode) stream via a temp multipart body on disk, reducing memory use for large files.

## [0.3.0] - 2026-08-17

First tagged release of the MATLAB client (`DB.m`).

- `DB.from_config()` — client from `config.json` next to `DB.m`.
- `DB(api_url, api_key)` — client with an explicit URL and token.
- `health()` — API health check.
- `get_streams()` — list streams (always a cell array, including when empty).
- `create_stream(name, ...)` — create a stream (`Type`, `Plugin`, `PluginArgs`, …).
- `get_datasets(stream_name)` — list datasets in a stream.
- `get_signals(stream_name, dataset_id)` — list signals on a dataset.
- `add_dataset(stream_name, dataset_name, ...)` — empty dataset (`Metadata`).
- `push_file(stream_name, file_path, ...)` — upload a file (`Metadata`, `FileName`, `Overwrite`).
- `wait_for_import(stream_name, dataset_id, ...)` — poll until import finishes (`Timeout`).
- `add_signal(stream_name, dataset_id, name, data, ...)` — upload a table/timetable (`Metadata`, `Overwrite`, `Priority`).
- `update_metadata(stream_name, dataset_id, metadata)` — merge dataset metadata.
- `get_data(dataset_path, signal_name)` — download a signal as a table (`time` in ns).
- `clear_cache()` — delete the local parquet cache.

[0.3.1]: https://github.com/marpledata/marple-sdk/releases/tag/matlab-v0.3.1
[0.3.0]: https://github.com/marpledata/marple-sdk/releases/tag/matlab-v0.3.0
