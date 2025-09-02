# Marple SDK

An SDK to interact with [Marple](https://www.marpledata.com) products.

## Installation and importing

Install the Marple SDK using your package manager:

- `poetry add marpledata`
- `uv add marpledata`
- `pip install marpledata`

The SDK can interact with three Marple products:

```python
from marple import Marple  # deprecated old SDK
from marple import DB  # Marple DB
from marple import Insight  # Marple Insight
```

## Marple DB

To get started, make sure you set up Marple DB in the user interface. Create

1. A **datastream**, to configure what kind of files you want to import
2. An **API token** (in user settings)

⚠ If you are using a VPC or self-hosted version, you should also submit a custom `api_url` to the `DB` object.

### Example: importing a file

This example shows the primary flow of importing a new file into Marple DB:

```python
import time
from marple import DB

# create a datastream and API token in the Marple DB web application
DATASTREAM = 'Car data'
API_TOKEN = '<your api token>'

db = DB(API_TOKEN)

if not db.check_connection()
  raise Exception("Could not connect")

id = db.push_file(DATASTREAM, "tests/example_race.csv", metadata={"driver": "Mbaerto"})

is_importing = True
while is_importing:
    status = db.get_status(STREAM_CSV, dataset_id)
    if status["import_status"] in ["FINISHED", "FAILED"]:
        is_importing = False
    time.sleep(1)

```

### Available functions

Functions are available for common actions.

**`db.get_streams()`**

Returns

- List of all datastreams, their configuration, and statistics about their data sizes.

**`db.get_datasets(stream_name)`**

Requires

- `stream_name`: Name of an existing datastream

Returns

- List of all datasets, their import status, and detailed statistics.

**`db.push_file(stream_name, file_path, metadata)`**

Requires

- `stream_name`: Name of an existing datastream
- `file_path`: Path to a local file on disk, e.g. `~/Downloads/test_data.mat`
- `metadata`: Dictionary with key-value pairs, e.g. `{'location': 'Munich', 'machine': 'C-3PO'}`

Returns

- Id of the new dataset

**`db.get_status(stream_name, dataset_id)`**

- `stream_name`: Name of an existing datastream
- `dataset_id`: Id of a dataset, obtained using e.g. `db.push_file(...)`

**`db.download_original(stream_name, dataset_id, destination)`**

Requires

- `stream_name`
- `dataset_id`
- `destination` (_optional_): the target directory on your local machine (default '.')

Returns

- Nothing, but downloads the file to destination folder

### Calling endpoints

For more advanced use cases, you can directly call endpoints by their METHOD:

```python
db.get('/health')
db.post('/stream/4/dataset/67/metadata', json={'Driver': 'Don Luigi'})
```

The full list of endpoints can be found in the Swagger Documentation: [https://db.marpledata.com/api/docs](https://db.marpledata.com/api/docs).

## Marple Insight

### Example: generating a MAT file export from a Marple DB file

```python
from marple import DB, Insight

INSIGHT_TOKEN = "<your api token>"
DB_TOKEN = "<your api token>"
DATASTREAM = "Car data"

insight = Insight(INSIGHT_TOKEN)
db = DB(DB_TOKEN)

stream_id = db._stream_name_to_id(DATASTREAM)
dataset = db.get_datasets(DATASTREAM)[0]

insight.export_mdb(stream_id, dataset["id"], format="h5", destination="/home/nero/Downloads")
```

### Available functions

**`db.export_db(stream_name, file_path, metadata)`**

Requires

- `stream_id`
- `dataset_id`
- `format`: File format, one of [mat, h5]
- `timestamp_start` (_optional_): Left time cutoff for export
- `timestamp_stop` (_optional_): Right time cutoff for export
- `destination` (_optional_): the target directory on your local machine (default '.')

Returns

- Nothing, but downloads a file "export.ext" to destination folder, with ".ext" depending on your export format (e.g. export.mat, export.h5, export.csv)
