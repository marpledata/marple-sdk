# MATLAB SDK Guide

This directory contains a small MATLAB client for Marple DB.

## Structure

- `DB.m`: MATLAB DB client implementation.
- `example.m`: example usage script.
- `add_signal_test.m`: manual wind-turbine `add_signal` smoke test.
- `config.json`: local configuration read by `DB.from_config()`.
- `README.md`: setup, quickstart, cache, and compatibility notes.

## Usage

- Add this directory to the MATLAB path before using the client:
  `addpath(genpath(fullfile(pwd, 'matlab')))`
- Create a client with `DB.from_config()` when using `config.json`.
- Run the example from the repo root with `run(fullfile('matlab', 'example.m'))`.

## Conventions

- Treat `config.json` as local configuration. Do not commit real API keys,
  workspaces, datapools, or deployment-specific URLs.
- `DB.from_config()` should continue reading `config.json` next to `DB.m`.
- `get_data(dataset_path, signal_name)` downloads parquet files into
  `_marplecache/<workspace>/<datapool>/dataset=<id>/signal=<id>/`.
- `add_dataset` creates an empty dataset; on file streams pair it with
  `add_signal` for custom lake writes (prefer file upload otherwise).
- `add_signal(stream_name, dataset_id, name, data, ...)` accepts a table or
  timetable only; it stages Snappy Parquet locally and uses
  `parquet-transcode prepare-upload` before the storage PUT.
- Preserve `clear_cache()` behavior for forcing a clean re-download.
- Older MATLAB versions may not read ZSTD-compressed Parquet. Keep the
  `parquet-transcode` helper download, directory transcode, and
  `prepare-upload` flow compatible with `rust/parquet-transcode/`.
  `TRANSCODE_VERSION` in `DB.m` must pin a release that includes
  `prepare-upload`.
- Keep generated caches, downloaded helper binaries, and local data out of
  source changes unless they are intentionally documented fixtures.
