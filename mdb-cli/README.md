# `mdb`

`mdb` is a command line client for the Marple DB API.

## Usage

The `mdb-cli` CLI tool (`mdb`) provides command-line access to the MarpleDB API: managing streams, uploading files, etc..

## 1. Installing Rust

The CLI is written in Rust. To get started, install Rust using [rustup](https://rustup.rs/):

```sh
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Follow the on-screen instructions to complete the installation. Afterward, ensure `cargo` is available:

```sh
cargo --version
```

## 2. Compiling the Binary

Navigate to the `cli` directory and build the binary with Cargo:

```sh
cd cli
cargo build --release
```

This will produce the binary in `target/release/mdb`.

## 3. Installing the Binary

To make `mdb` available system-wide, you can copy it to a directory in your `PATH`, for example:

```sh
cp target/release/mdb ~/.cargo/bin/
```

Or, install directly using Cargo:

```sh
cargo install --path .
```

## 4. Using `mdb help`

To see available commands and usage information, run:

```sh
mdb help
```

This will display a list of all supported commands and options for the CLI tool. The tool uses the `MDB_HOST` & `MDB_TOKEN` environment variables to connect to a MarpleDB instance.

As an example:

```sh
EXPORT MDB_HOST=db.app.marpledata.com
EXPORT MDB_TOKEN=mdb_abcdef...ghijk
mdb stream list
mdb stream new "MDF Stream" plugin=mdf plugin-args="--chunksize 2e8"
mdb ingest "MDF Stream" --recursive --extension .mf4 mdf_files/
mdb ingest "MDF Stream" -re .mf4 mdf_files/
```
