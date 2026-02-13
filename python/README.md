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

### Examples
#### Import a file and wait for it to import

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

stream = db.get_stream(STREAM)
dataset = stream.push_file("examples_race.csv", metadata={"driver": "Mbaerto"})
# Wait at most 10s for the dataset to completely import and get the new state of the dataset
dataset = dataset.wait_for_import(timeout=10)
```

#### Filter datasets and get resampled data
```python
# See previous example for setup
import re
from marple.db import Dataset

datasets = stream.get_datasets()  # Get all datasets in a specific Data Stream
# OR
# datasets = db.get_datasets()  # Get all datasets in the datapool

# Keep all datasets where car_id is 1 or 2 and track is track_1
datasets = datasets.where_metadata({"car_id": [1, 2], "track": "track_1"})
# Wait until all datasets are imported (to ensure all signals are available for the next step)
datasets = datasets.wait_for_import()
# Keep only datasets that have been succesfully imported
datasets = datasets.where_imported()
# Keep only datasets that have a car.speed signal and the max value of this signal is greater than 75
datasets = datasets.where_signal("car.speed", "max", greater_than=75)
datasets = datasets.where_signal("car.engine.temp", "mean", greater_than=30)
# Keep only datasets with sufficient datapoints
datasets = datasets.where_dataset("n_datapoints", greater_than=100000)

def custom_filter_function(dataset: Dataset) -> bool:
    return (
        dataset.metadata.get("weather") == "sunny"
        or dataset.get_signal("car.engine.NGear").stats.get("avg", 0) ** 2 > 16
    )

# Pass any function to filter the datasets on more complex conditions
datasets = datasets.where(custom_filter_function)

# Create an overview of the datasets as a pandas.DataFrame to save it to a CSV.
datasets.to_pandas().to_csv("all_datasets.csv")

# Get a dataframe per dataset of the matching signals which is resampled at a period of 0.17s.
# The regex patterns will match with car.wheel.rear.left.speed, car.wheel.rear.front.speed, ...
for dataset, data in datasets.get_data(
    signals=[
        "car.speed",
        "car.engine.temp",
        re.compile("car.wheel.*.speed"),
        re.compile("car.wheel.*.trq"),
    ],
    resample_rule="0.17s",
):
    machine_learning_model.train(data)
```

#### Delete a dataset that failed to import
```python
datasets = stream.get_datasets()
datasets = datasets.where_dataset("import_status", equals="FAILED")

# datasets is of type DatasetList which is a subclass of list so you can do all normal list operations on it.
if len(datasets) > 0:
    datasets[0].delete()
```

### Common operations

- **List streams**: `db.get_streams()`
- **List datasets in a stream**: `stream.get_datasets()`
- **Upload a file to a file-stream**: `stream.push_file(file_path, metadata={...})`
- **Wait for a dataset to import**: `dataset.wait_for_import(timeout=60)`
- **Download original uploaded file**: `dataset.download(destination_folder=".")`
- **Download parquet for a signal**: `dataset.get_signal(signal_name).download(destination_folder=".")`
- **Get a resampled df of multiple signals**: `dataset.get_data(signals=[...], resample_rule="1s")`

For live/realtime streams (creating and appending data):

- **Create an empty dataset**: `db.add_dataset(stream_key, dataset_name, metadata=None)`
- **Upsert signal definitions**: `db.upsert_signals(stream_key, dataset_id, signals=[...])`
- **Append timeseries data**: `db.dataset_append(stream_key, dataset_id, data=df, shape="long"|"wide"|None)`

### Calling endpoints directly

For advanced use cases, you can call API endpoints directly:

```python
db.get("/health")
```

### Notes on DB API changes

- Methods like `DB.push_file`, `DB.download_signal`, and `DB.update_metadata` are deprecated.
- Prefer stream/dataset methods instead: `stream.push_file`, `dataset.get_signal(...).download()`, and `dataset.update_metadata(...)`.
- These are still available for compatibility, but the examples above use the current API.

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

dataset_id = db.get_datasets(STREAM)[0].id
insight_dataset = insight.get_dataset_mdb(dataset_id)

file_path = insight.export_data_mdb(
    dataset_id,
    format="h5",
    signals=["car.speed"],
    destination=".",
)
print("Wrote", file_path)
```
