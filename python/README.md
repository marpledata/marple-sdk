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

### Calling endpoints

For more advanced use cases, you can directly call endpoints by their METHOD:

```python
db.get('/health')
db.post('/stream/4/dataset/67/metadata', json={'Driver': 'Don Luigi'})
```

The full list of endpoints can be found in the Swagger Documentation: [https://db.marpledata.com/api/docs](https://db.marpledata.com/api/docs).
