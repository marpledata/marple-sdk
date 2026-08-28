# `mdb`

`mdb` is a command-line client for the MarpleDB API, providing direct access to manage streams, ingest files, query datasets, browse a workspace in a terminal UI, and interact with the MarpleDB service from your terminal.

## Installation

From crates.io:

```sh
cargo install mdb-cli
```

From this repository:

```sh
cargo install --path rust/mdb-cli
```

If installing from the `rust/` workspace directory:

```sh
cargo install --path mdb-cli
```

## Authentication

Create an API token in the MarpleDB web application and provide it through `MDB_TOKEN` or `--mdb-token`:

```sh
export MDB_TOKEN="mdb_your_token_here"
export MDB_URL="https://db.marpledata.com/api/v1"
```

`MDB_URL` is optional and defaults to `https://db.marpledata.com/api/v1`. For VPC or self-hosted deployments, set `MDB_URL` or pass `--mdb-url`; the value should usually end in `/api/v1`.

Dataset commands also accept `MDB_STREAM` or `--stream` for the stream name.

The CLI automatically loads `.env` from the current directory when present. Use `--env-file` to choose another dotenv file before credentials are read:

```sh
mdb --env-file .env.staging stream list
mdb --env-file .env.production datapool datasets
```

Exported shell variables take precedence over values in dotenv files, and explicit CLI flags such as `--mdb-token` and `--mdb-url` take precedence over both.

## Command Overview

- `mdb ping` checks API health and token validity.
- `mdb stream list` lists streams.
- `mdb stream get <stream-name>` prints one stream.
- `mdb stream new <stream-name> [key=value ...]` creates a stream.
- `mdb stream update <stream-name> [key=value ...]` updates stream properties.
- `mdb stream delete <stream-name>` deletes a stream and all of its datasets (admin token required).
- `mdb ingest <stream-name> [options] <files-or-directories>...` uploads files.
- `mdb dataset list --stream <stream-name> [--format short|long]` lists datasets in a stream.
- `mdb dataset get <dataset> --stream <stream-name>` prints one dataset by path or id.
- `mdb dataset download [--output-dir DIR] [<dataset>] --stream <stream-name>` downloads original uploaded files; omit the dataset to download all in the stream.
- `mdb dataset reingest <dataset> --stream <stream-name>` re-queues a dataset for ingest; poll with `mdb dataset get`.
- `mdb dataset debug <dataset> --stream <stream-name>` prints ingest debug messages.
- `mdb dataset delete <dataset> --stream <stream-name>` deletes a dataset.
- `mdb datapool datasets [--pool POOL] [--queue] [--format short|long]` lists datapool datasets.
- `mdb get`, `mdb post`, and `mdb delete` call raw API endpoints.
- `mdb` (no subcommand) opens a stream / dataset / signal browser when stdin and stdout are a terminal; otherwise it prints help. `mdb browse` opens the same browser explicitly. It loads `./.env` from the current directory, or the path last saved in `$XDG_CONFIG_HOME/mdb/browse.toml` (typically `~/.config/mdb/browse.toml`). Pass `--env-file` to choose another file, or press `v` to browse local folders, type a path, or pick a recently used file (shown with its workspace name). The chosen file is saved for the next session. Press `/` to filter the focused table.

## Examples

### Complete Workflow

```sh
mdb --help

# Set up credentials
export MDB_TOKEN="mdb_your_token_here"
export MDB_URL="https://db.marpledata.com/api/v1"

# Check connection
mdb ping

# List existing streams
mdb stream list

# Create a new stream
mdb stream new "Test Stream" plugin=csv

# Ingest a CSV file
mdb ingest "Test Stream" data.csv

# Ingest a CSV file with dataset metadata
mdb ingest "Test Stream" -m Deployment=prod -m Foo=Bar data.csv

# List datasets in the stream
mdb dataset list --stream "Test Stream"

# Print dataset list as JSON
mdb dataset list --stream "Test Stream" --format long

# List all datasets in the default datapool
mdb datapool datasets

# List datasets currently in the ingest queue
mdb datapool datasets --queue

# Get dataset details (path or id)
mdb dataset get data.csv --stream "Test Stream"

# Query the data
mdb post "/query" query="select path, stream_id, metadata from mdb_default_dataset limit 1;"

# Download the original file
mdb dataset download --output-dir ./backups data.csv --stream "Test Stream"

# Print ingest debug messages
mdb dataset debug data.csv --stream "Test Stream"

# Re-queue the original file for ingest; poll with `mdb dataset get`
mdb dataset reingest data.csv --stream "Test Stream"

# Delete a dataset
mdb dataset delete data.csv --stream "Test Stream"

# Delete a stream and all of its datasets (admin token required)
mdb stream delete "Test Stream"
```

### Batch Ingestion

```sh
# Ingest all CSV files in a directory
mdb ingest "Data Stream" --recursive --extension csv ./data_directory/

# Ingest MDF files, skipping already uploaded ones
mdb ingest "MDF Stream" --recursive --extension mf4 --skip-existing ./mdf_files/

# Ingest recursively with shared metadata applied to each uploaded file
mdb ingest "Metrics" -m Deployment=value -m Foo=Bar --recursive --skip-existing ./data_directory/
```

### Metadata Values

Metadata and generic endpoint arguments use `key=value` syntax. Values are parsed as JSON when possible and otherwise kept as strings:

```sh
# String values
mdb ingest "Runs" -m driver=Mbaerto run.csv

# JSON values
mdb ingest "Runs" -m run=42 -m validated=true -m tags='["race","test"]' run.csv
```

The same parser is used for stream properties and generic endpoint arguments.

### Ingestion Options

```sh
# Recursively ingest all CSV files below a directory
mdb ingest "Runs" --recursive --extension csv ./data/

# Skip files whose filename already exists as a dataset path in the stream
mdb ingest "Runs" --recursive --skip-existing ./data/

# Increase concurrent direct-storage part uploads
mdb ingest "Runs" --concurrency 8 big-file.mf4

# Force upload through the MarpleDB API server
mdb ingest "Runs" --upload-mode server run.csv
```

`--upload-mode auto` lets the server choose the best upload mode. `--upload-mode server` forces uploads through the API server, which can be useful for debugging or network environments that cannot reach direct storage URLs.

### Downloads

`mdb dataset download <dataset> --stream <stream>` downloads the original uploaded file backup for one dataset. `<dataset>` can be a path (`run.csv`) or a numeric id. If the dataset is omitted, the CLI attempts to download all datasets in the stream:

```sh
mdb dataset download --output-dir ./backups 12345 --stream "Runs"
mdb dataset download --output-dir ./backups run.csv --stream "Runs"
mdb dataset download --output-dir ./backups --stream "Runs"
```

Downloads use pre-signed storage links internally.

### Dataset Lists

Dataset list commands print a tabular view by default. Use `--format long` when you need the full JSON response for scripting:

```sh
mdb dataset list --stream "Runs"
mdb dataset list --stream "Runs" --format long
mdb datapool datasets --pool default
mdb datapool datasets --pool default --queue
mdb datapool datasets --pool default --format long
```

Regular dataset tables show stored-data columns such as cold, hot, and backup bytes. Queue tables replace cold and hot bytes with import progress:

```sh
mdb datapool datasets --pool default --queue
```

### Generic Endpoints

Use `get`, `post`, and `delete` for endpoints not yet covered by first-class commands:

```sh
mdb get "/health"
mdb post "/query" query="select path, stream_id, metadata from mdb_default_dataset limit 1;"
mdb delete "/stream/123/delete"
```

Arguments after the endpoint become query parameters for `get` and JSON body fields for `post` and `delete`.

## Troubleshooting

- **Invalid token**: create a new API token in the MarpleDB UI and set `MDB_TOKEN`.
- **API is not responding**: check `MDB_URL`; it should usually include `/api/v1`.
- **Stream not found**: run `mdb stream list` and check the exact stream name.
- **Import failed**: inspect `mdb dataset get <dataset> --stream <stream>` for `import_status` and `import_message`.
- **Download failed**: the dataset may not have an original-file backup available.

## Getting Help

- **Bug reports and feature requests**: Please open an issue on [GitHub](https://github.com/marpledata/marple-sdk/issues)
- **Documentation**: See the [MarpleDB documentation](https://docs.marpledata.com/docs)
- **General questions**: Contact us through our [website](https://www.marpledata.com/)
