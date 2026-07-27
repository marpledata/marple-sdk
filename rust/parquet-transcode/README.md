# parquet-transcode

Helper binary for the MATLAB Marple DB SDK.

1. **Download compatibility** — recursively rewrite ZSTD Parquet files to
   Snappy so older MATLAB releases can read cached signal data.
2. **Upload preparation** — convert MATLAB staging Parquet
   (`time` / `value` / `value_text`) into the exact Iceberg lake schema
   used by Marple DB signal uploads (field IDs, ZSTD, identity columns).

## Usage

### Directory transcode (legacy / download path)

```
parquet-transcode <directory>
```

Recursively walks `<directory>`, finds all `.parquet` files, and
re-compresses any ZSTD-encoded columns to Snappy in place. Compatible
files are skipped.

### Prepare upload

```
parquet-transcode prepare-upload \
  --input <staging.parquet> \
  --output <upload.parquet> \
  --dataset-id <id> \
  --signal-id <id> \
  [--expected-rows <n>]
```

Writes the authoritative five-column lake Parquet and prints JSON to
stdout:

```json
{"output":".../upload.parquet","rows":1000,"size":123456,"footer":789}
```

Diagnostics go to stderr.

## Releases

Pre-built binaries are published as GitHub Releases under the tag
`parquet-transcode-v<VERSION>`.

Binary naming convention:

```
parquet-transcode-v0.1.0-darwin-arm64
parquet-transcode-v0.1.0-darwin-x64
parquet-transcode-v0.1.0-linux-x64
parquet-transcode-v0.1.0-windows-x64.exe
```

The MATLAB SDK (`matlab/DB.m`) automatically downloads the correct
binary on first use.

## Building and testing

```
cargo build --release
cargo test --locked
PARQUET_TRANSCODE_BIN=target/debug/parquet-transcode \
  uv run --with 'pyarrow==24.0.0' tests/oracle_prepare_upload.py
```
