# Marple SDK

Powerful SDKs for [Marple](https://www.marpledata.com) products.

All documentation is available on [marpledata.gitlab.io/marple-sdk/](https://marpledata.gitlab.io/marple-sdk/).

## Python SDK

The Python SDK is published on PyPI as **`marpledata`**: [`https://pypi.org/project/marpledata/`](https://pypi.org/project/marpledata/).

For Python-specific usage, development, and examples in this repo, see [`python/README.md`](python/README.md).

## Rust SDK

The Rust SDK is published on crates.io as **`marple-db`** and imported in Rust code as `marple_db`.

For Rust SDK installation, usage, and examples, see [`rust/mdb-sdk/README.md`](rust/mdb-sdk/README.md).

## Rust CLI

The `mdb` command-line tool provides direct access to the MarpleDB API from your terminal. The CLI crate is published on crates.io as **`mdb-cli`** and installs the `mdb` binary.

For CLI installation, usage, and examples, see [`rust/mdb-cli/README.md`](rust/mdb-cli/README.md).

## MATLAB SDK

You can use [MATLAB online](https://matlab.mathworks.com/) for testing.
It has a free tier for 20h of MATLAB / month.

For MATLAB-specific usage and examples in this repo, see [`matlab/README.md`](matlab/README.md).

## Parquet Transcoder

`parquet-transcode` is a helper binary used by the MATLAB SDK to convert ZSTD-compressed Parquet files to Snappy for older MATLAB versions.

For usage, build, and release details, see [`rust/parquet-transcode/README.md`](rust/parquet-transcode/README.md).
