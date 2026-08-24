# Rust CLI Guide

This crate provides the `mdb` command-line client for the MarpleDB API. It wraps
the Rust SDK crate `marple-db`.

## Structure

- `src/main.rs`: clap command definitions, argument parsing, command handlers,
  progress output, and process exit behavior.
- `src/browse/`: stream / dataset / signal browser started by `mdb` (TTY) or
  `mdb browse`. Session (env file and last stream) is saved in
  `$XDG_CONFIG_HOME/mdb/browse.toml`. The env-file modal lives in `picker.rs`.
- `src/table.rs`: shared windowed table draw plus `/` substring row search used
  by browse.
- `tests/integration.rs`: CLI tests using `assert_cmd`.
- `tests/unit.rs`: table-format unit tests.
- `README.md`: user-facing CLI documentation and examples.
- `CONTRIBUTING.md`: contributor setup notes.
- `Cargo.toml`: CLI package metadata, binary declaration, and dependencies.

## Commands

- Run help from `rust/`: `cargo run -p mdb-cli -- --help`
- Test this crate from `rust/`: `cargo test -p mdb-cli --locked`
- Install from repo root: `cargo install --path rust/mdb-cli`
- Install from `rust/`: `cargo install --path mdb-cli`

## Conventions

- CLI command definitions should stay close to their handlers in `src/main.rs`
  unless the file is intentionally split into modules. The browser is split on
  purpose (`src/browse/`).
- User-facing command output belongs in the CLI crate, not the SDK crate.
- Write CLI behavior tests in `tests/integration.rs`. Use `assert_cmd` to spawn
  the `mdb` binary and assert on stdout, stderr, and exit status.
- Keep progress bars and colored output out of JSON stdout paths so scripts can
  parse command responses reliably.
