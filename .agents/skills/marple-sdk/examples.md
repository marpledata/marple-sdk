# Marple SDK examples

End-to-end recipes for the `marpledata` Python SDK (Marple DB). Adapt signal names, time windows, and filters to the actual task; these are starting points, not fixed templates.

## Recipe 1: import a file and wait for import

```python
import os
from marple import DB

db = DB(os.environ["MDB_TOKEN"])
db.check_connection()

stream = db.get_stream("Car data")
dataset = stream.push_file("examples_race.csv", metadata={"driver": "Mbaerto"})
# Importing is async; wait before reading data.
dataset = dataset.wait_for_import(timeout=10)
```

## Recipe 2: filter datasets and get resampled data

```python
import os, re
from marple import DB

db = DB(os.environ["MDB_TOKEN"])
stream = db.get_stream("Car data")

datasets = stream.get_datasets()
datasets = datasets.where_metadata({"car_id": [1, 2], "track": "track_1"})
datasets = datasets.wait_for_import().where_imported()
datasets = datasets.where_signal("car.speed", "max", greater_than=75)

for dataset, data in datasets.get_data(
    signals=["car.speed", re.compile(r"car\.wheel\..*\.speed")],
    resample_rule="0.17s",
):
    ...  # data is a DataFrame on a common timebase
```

## Recipe 3 (happy path): question to self-contained script + visualization

A general question such as "what happened in dataset xyz in corner 3?" is enough to produce a self-contained script that connects, fetches the relevant signals/window, and renders a plot. Refine signals, the time window, and the plot type together with the user instead of hardcoding guesses.

Default: a single-file `uv` script with PEP 723 inline dependencies. Run with `uv run analyze.py`.

```python
# /// script
# requires-python = ">=3.10"
# dependencies = ["marpledata", "pandas", "plotly"]
# ///
import os
from marple import DB
from plotly.subplots import make_subplots
import plotly.graph_objects as go

STREAM = "Car data"
DATASET = "xyz"
SIGNALS = ["car.speed", "car.steering_angle"]  # refine for the actual question

db = DB(os.environ["MDB_TOKEN"])
db.check_connection()

stream = db.get_stream(STREAM)
dataset = next(d for d in stream.get_datasets().wait_for_import().where_imported()
               if d.path == DATASET)  # datasets are identified by path, not name

# Optionally narrow to the time window of interest (e.g. corner 3) once known.
data = dataset.get_data(signals=SIGNALS, resample_rule="0.05s")

fig = make_subplots(rows=len(SIGNALS), cols=1, shared_xaxes=True,
                    subplot_titles=SIGNALS)
for i, sig in enumerate(SIGNALS, start=1):
    fig.add_trace(go.Scatter(x=data.index, y=data[sig], name=sig), row=i, col=1)
fig.update_layout(height=300 * len(SIGNALS), title=f"{DATASET} analysis")

# Standalone, self-contained HTML dashboard (no server, no CDN needed).
fig.write_html("dashboard.html", include_plotlyjs="inline")
print("Wrote dashboard.html")
```

Fallback without `uv`: drop the `# /// script` header and provide a `requirements.txt`:

```
marpledata
pandas
plotly
```

```bash
pip install -r requirements.txt && python analyze.py
```

## Recipe 4: uploading large files and realtime (brief)

For large uploads and realtime streaming, consult the docs rather than inlining here:

- Upload large files: https://docs.marpledata.com/docs/sdk/overview/python-sdk
- Realtime stream via DB SDK: https://docs.marpledata.com/docs/sdk/advanced/realtime-stream

When unsure of the exact method, introspect the package or query `<doc-url>.md?ask=<question>` (see `reference.md`).
