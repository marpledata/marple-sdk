# Changelog

All notable changes to `mdb` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Browse env-file loads that omit `MDB_URL` now default to SaaS instead of keeping a leftover staging URL from the process environment.
- Dataset, stream, and signal ids from the SDK are `i64`.
- Browse inspect (`i`) scrolls with `j`/`k` and `gg`/`G` instead of moving the table selection.
- A saved env file that fails to load now shows the error in the status line.
- Browse upload and download Enter on `../` goes up a folder.
- Browse polls import status for the painted dataset window (and this-session uploads), so `PROCESSING` keeps updating after switching workspace or opening a stream with in-flight imports.
- Browse import-status polling no longer blocks keyboard input when the status endpoint is slow.
- Browse debug view splits ingest log strings on newlines so multi-line messages scroll as separate rows.
- Browse env-file picker is `w`; the status line and README previously said `v`.

### Changed

- Browse TUI inherits the terminal foreground instead of grey/white, so text stays readable in light terminals.
- Browse no longer restores the last open stream; restarting `mdb browse` lands on the workspace stream list.
- Browse stream and dataset details sit under the left list; the child table uses the full right column. Opening a dataset shows info and focuses that pane; `→` cycles debug messages and signals (and keeps focus there). `←` returns to the dataset list. Press `i` to expand inspect on a stream, or jump to dataset info.
- Only the focused pane uses the blue row cursor.
- Dataset commands take `--stream` (or `MDB_STREAM`) instead of a leading stream name: `mdb dataset list --stream Metrics`, `mdb dataset get xyz.metrics --stream Metrics`.
- Dataset `get`, `download`, `reingest`, `debug`, and `delete` accept a dataset path or numeric id.
- Dataset status output uses SDK `ImportStatus` names, including `UNKNOWN`.
- Dataset downloads use `MarpleDB::download_original_with_progress` instead of the SDK storage client.
- Browse `/` filter matches id, name/path, status, and plugin text instead of formatted table cells, and import totals are cached so large dataset lists stay responsive.

### Added

- Browse `p` reruns aliasing and script processing for the selected datasets (or every dataset in the stream).
- `mdb stream delete`, `mdb dataset … delete`, `mdb dataset … reingest`, and `mdb dataset … debug`.
- Optional `native-tls` Cargo feature for SChannel / Secure Transport / OpenSSL (the SDK default already uses rustls with OS certificate roots).

- `mdb` and `mdb browse` open a stream / dataset / signal browser (bare `mdb` only when stdin and stdout are a terminal; otherwise help is printed). Press `w` for an env-file picker (folders, typed path, recent files labeled by workspace). Session is saved in `$XDG_CONFIG_HOME/mdb/browse.toml`. The workspace card shows license and usage.
- Browse `u` uploads files or folders into the selected file stream. Enter/space selects, Shift+Enter selects the range from the last Enter/space, `a` selects all in the current folder. Enter on `../` goes up a folder. The modal counts files vs folders, and has overwrite, skip-existing, and extension options. The dataset table shows upload then import progress until the dataset is ready. `POSTPROCESSING` is shown as `PROCESSING`.
- Browse `d` downloads original files: enter/space select datasets, Shift+Enter selects the range from the last Enter/space, `a` selects all visible rows, `d` on a stream downloads the whole stream. A folder picker chooses the destination (Enter on `../` goes up); the dataset table shows a `DOWNLOADING` progress overlay.
- Browse `x` deletes the selected datasets (enter confirms). Browse `r` re-queues them for ingest from the original file.
- Opening a dataset in browse shows general info and focuses that pane; `→` cycles through ingest debug messages and signals. `←` returns to the dataset list. `/` filters debug lines when that view is focused.
- `/` filters the focused table (case-insensitive substring of id, name/path, status, and plugin). The `/` prompt is visible while editing; Enter keeps the filter, Esc cancels the edit, Esc again clears it. Long tables window the visible rows (`1–20 of 180`).

## [0.3.0] - 2026-08-20

### Added

- `mdb ingest --overwrite` to replace existing datasets with the same name (mutually exclusive with `--skip-existing`).

### Changed

- Dataset status output renders `COOLING` and `COOLING_FAILED`.
- Depends on `marple-db` 0.3.0.

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

[0.3.0]: https://github.com/marpledata/marple-sdk/releases/tag/mdb-cli-v0.3.0
[0.2.1]: https://github.com/marpledata/marple-sdk/releases/tag/mdb-cli-v0.2.1
[0.2.0]: https://github.com/marpledata/marple-sdk/releases/tag/mdb-cli-v0.2.0
[0.1.6]: https://github.com/marpledata/marple-sdk/releases/tag/mdb-cli-v0.1.6
[0.1.1]: https://github.com/marpledata/marple-sdk/releases/tag/mdb-cli-v0.1.1
[0.1.0]: https://github.com/marpledata/marple-sdk/releases/tag/mdb-cli-v0.1.0
