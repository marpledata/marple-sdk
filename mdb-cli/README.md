# `mdb`

`mdb` is a command-line client for the MarpleDB API, providing direct access to manage streams, ingest files, query datasets, and interact with the MarpleDB service from your terminal.

## Examples

### Complete Workflow

```sh
mdb --help

# Set up credentials
export MDB_TOKEN="mdb_your_token_here"

# Check connection
mdb ping

# List existing streams
mdb stream list

# Create a new stream
mdb stream new "Test Stream" plugin=csv

# Ingest a CSV file
mdb ingest "Test Stream" data.csv

# List datasets in the stream
mdb dataset "Test Stream" list

# Get dataset details (use the ID from the list)
mdb dataset "Test Stream" get 12345

# Query the data
mdb post "/query" query="select path, stream_id, metadata from mdb_default_dataset limit 1;"

# Download the original file
mdb dataset "Test Stream" download --output-dir ./backups 12345
```

### Batch Ingestion

```sh
# Ingest all CSV files in a directory
mdb ingest "Data Stream" --recursive --extension csv ./data_directory/

# Ingest MDF files, skipping already uploaded ones
mdb ingest "MDF Stream" --recursive --extension mf4 --skip-existing ./mdf_files/
```

## Getting Help

- **Bug reports and feature requests**: Please open an issue on [GitHub](https://github.com/marpledata/marple-sdk/issues)
- **Documentation**: See the [MarpleDB documentation](https://docs.marpledata.com/docs)
- **General questions**: Contact us through our [website](https://www.marpledata.com/)
