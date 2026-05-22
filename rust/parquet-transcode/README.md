# parquet-transcode

Transcodes ZSTD-compressed Parquet files to Snappy compression for compatibility with older MATLAB versions that don't support ZSTD.

## Usage

```
parquet-transcode <directory>
```

Recursively walks `<directory>`, finds all `.parquet` files, and re-compresses any ZSTD-encoded columns to Snappy (in-place). Files already using a compatible codec are skipped.

## Releases

Pre-built binaries are published as GitHub Releases under the tag `parquet-transcode-v<VERSION>`.

Binary naming convention:

```
parquet-transcode-v0.1.0-darwin-arm64
parquet-transcode-v0.1.0-darwin-x64
parquet-transcode-v0.1.0-linux-x64
parquet-transcode-v0.1.0-windows-x64.exe
```

The MATLAB SDK (`matlab/DB.m`) automatically downloads the correct binary on first use.

## Building

```
cargo build --release
```
