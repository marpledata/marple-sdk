# MATLAB SDK Guide

This directory contains a small MATLAB client for Marple DB.

## Structure

- `DB.m`: MATLAB DB client implementation.
- `example.m`: example usage script.
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
- Preserve `clear_cache()` behavior for forcing a clean re-download.
- Older MATLAB versions may not read ZSTD-compressed Parquet. Keep the
  `parquet-transcode` helper download and transcoding flow compatible with
  `rust/parquet-transcode/`.
- Keep generated caches, downloaded helper binaries, and local data out of
  source changes unless they are intentionally documented fixtures.
