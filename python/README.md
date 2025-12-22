# Marple SDK

An SDK to interact with [Marple](https://www.marpledata.com) DB & Insight

## Installation and importing

Install the Marple SDK using your package manager:

- `poetry add marpledata`
- `uv add marpledata`
- `pip install marpledata`

The SDK currently exposes:

```python
from marple import DB      # Marple DB
from marple import Insight # Marple Insight
```

## Marple DB

To get started:

- Create a **stream** in the Marple DB UI
- Create an **API token** (in user settings)

If you are using a VPC or self-hosted version, pass a custom `api_url` to `DB(...)` (it should end in `/api/v1`).

### Example: import a file and poll ingest status

This is the typical flow for importing a new file into Marple DB:

```python
import time
from marple import DB

# Create a stream + API token in the Marple DB web application
STREAM = "Car data"
API_TOKEN = "<your api token>"
API_URL = "https://db.marpledata.com/api/v1"  # optional if using the default SaaS

db = DB(API_TOKEN, API_URL)

db.check_connection()

dataset_id = db.push_file(STREAM, "tests/examples_race.csv", metadata={"driver": "Mbaerto"})

while True:
    status = db.get_status(STREAM, dataset_id)
    if status.get("import_status") in {"FINISHED", "FAILED"}:
        break
    time.sleep(1)
```

### Common operations

- **List streams**: `db.get_streams()`
- **List datasets in a stream**: `db.get_datasets(stream_key)`
- **Upload a file to a file-stream**: `db.push_file(stream_key, file_path, metadata={...})`
- **Poll ingest status**: `db.get_status(stream_key, dataset_id)`
- **Download original uploaded file**: `db.download_original(stream_key, dataset_id, destination_folder=".")`
- **Download parquet for a signal**: `db.download_signal(stream_key, dataset_id, signal_id, destination_folder=".")`

For live/realtime streams (creating and appending data):

- **Create an empty dataset**: `db.add_dataset(stream_key, dataset_name, metadata=None)`
- **Upsert signal definitions**: `db.upsert_signals(stream_key, dataset_id, signals=[...])`
- **Append timeseries data**: `db.dataset_append(stream_key, dataset_id, data=df, shape="long"|"wide"|None)`

### Calling endpoints directly

For advanced use cases, you can call API endpoints directly:

```python
db.get("/health")
db.post("/query", json={"query": "select 1"})
```

## Marple Insight

### Common operations

- **List datasets in the workspace**: `insight.get_datasets()`
- **Get a Marple DB dataset (by dataset id)**: `insight.get_dataset_mdb(dataset_id)`
- **List signals in a dataset**: `insight.get_signals(dataset_filter)` / `insight.get_signals_mdb(dataset_id)`

### Example: export a dataset (H5/MAT)

```python
from marple import DB, Insight

INSIGHT_TOKEN = "<your api token>"
INSIGHT_URL = "https://insight.marpledata.com/api/v1"  # optional if using the default SaaS
DB_TOKEN = "<your api token>"
DB_URL = "https://db.marpledata.com/api/v1"  # optional if using the default SaaS
STREAM = "Car data"

insight = Insight(INSIGHT_TOKEN, INSIGHT_URL)
db = DB(DB_TOKEN, DB_URL)

dataset_id = db.get_datasets(STREAM)[0]["id"]
insight_dataset = insight.get_dataset_mdb(dataset_id)

file_path = insight.export_data_mdb(
    dataset_id,
    format="h5",
    signals=["car.speed"],
    destination=".",
)
print("Wrote", file_path)
```
