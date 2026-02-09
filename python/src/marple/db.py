import json
import re
from collections import UserList
from io import BytesIO
from pathlib import Path
from typing import Callable, Iterable, Literal, Optional
from urllib import parse, request

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from pydantic import BaseModel, PrivateAttr, ValidationError
from requests import Response

from marple.utils import validate_response

SAAS_URL = "https://db.marpledata.com/api/v1"

COL_TIME = "time"
COL_SIG = "signal"
COL_VAL = "value"
COL_VAL_TEXT = "value_text"

SCHEMA = pa.schema(
    [
        pa.field(COL_TIME, pa.int64()),
        pa.field(COL_SIG, pa.string()),
        pa.field(COL_VAL, pa.float64()),
        pa.field(COL_VAL_TEXT, pa.string()),
    ]
)


class DataStream(BaseModel):
    type: Literal["files", "realtime"]
    id: int
    name: str
    description: str | None
    datapool: str
    layer_shifts: list[int]
    version_id: int
    insight_workspace: Optional[str] = None
    insight_project: Optional[str] = None

    # Stats
    created_at: float
    last_updated: float
    last_ingested: Optional[float] = None
    n_datasets: Optional[int] = None
    n_datapoints: Optional[int] = None
    cold_bytes: Optional[int] = None
    hot_bytes: Optional[int] = None

    plugin: Optional[str] = None
    plugin_args: Optional[str] = None
    signal_reduction: Optional[list] = None

    _has_all_datasets: bool = PrivateAttr(default=False)
    _known_datasets: dict[str, int] = PrivateAttr(default_factory=dict)
    _datasets: dict[int, "Dataset"] = PrivateAttr(default_factory=dict)
    _db: "DB" = PrivateAttr()

    def __init__(self, db: "DB", **kwargs):
        super().__init__(**kwargs)
        self._db = db

    def get_dataset(self, id: int | None = None, path: str | None = None) -> "Dataset":
        """Get a specific dataset in this datastream by its ID or path."""

        if id is None and path is None:
            raise ValueError("Either id or path must be provided.")
        if id is not None and path is not None:
            raise ValueError("Only one of id or path can be provided.")

        id = id if path is None else self.get_dataset_id(path)

        if id is None:
            raise ValueError(f"Dataset with path {path} not found in datastream {self.name}.")

        if id not in self._datasets:
            r = self._db.get(f"/stream/{self.id}/dataset/{id}")
            dataset = Dataset(datastream=self, **r.json())
            self._datasets[id] = dataset
            self._known_datasets[dataset.path] = dataset.id
        return self._datasets[id]

    def get_dataset_id(self, dataset_path: str) -> int | None:
        if dataset_path not in self._known_datasets:
            self.get_datasets()
        return self._known_datasets.get(dataset_path)

    def get_datasets(self) -> "DatasetList":
        """Get all datasets in this datastream."""
        if not self._has_all_datasets:
            r = self._db.get(f"/stream/{self.id}/datasets")
            for dataset in r.json():
                try:
                    dataset_obj = Dataset(datastream=self, **dataset)
                except ValidationError as e:
                    raise UserWarning(
                        f"Failed to parse dataset with id {dataset.get('id')} and path {dataset.get('path')}. Skipping. Error: {e}"
                    )
                self._datasets[dataset_obj.id] = dataset_obj
                self._known_datasets[dataset_obj.path] = dataset_obj.id
            self._has_all_datasets = True
        return DatasetList(self._datasets.values())


class Dataset(BaseModel):
    id: int
    datastream_id: int
    datastream_version: int | None
    created_at: float
    created_by: str | None
    import_status: str
    import_progress: float | None
    import_message: str | None
    import_time: float | None
    path: str
    metadata: dict
    cold_path: str
    cold_bytes: int
    hot_bytes: int
    backup_path: str | None
    backup_size: int | None
    plugin: str
    plugin_args: str
    n_datapoints: int
    n_signals: int
    timestamp_start: int | None
    timestamp_stop: int | None
    import_speed: float | None
    parquet_version: int

    _db: "DB" = PrivateAttr()
    _known_signals: dict[str, int] = PrivateAttr(default_factory=dict)
    _signals: dict[int, "Signal"] = PrivateAttr(default_factory=dict)
    _has_all_signals: bool = PrivateAttr(default=False)

    datastream: DataStream

    def __init__(self, datastream: DataStream, **kwargs):
        super().__init__(datastream=datastream, **kwargs)
        self._db = datastream._db

    def get_signal(self, name: str | None = None, id: int | None = None) -> "Signal":
        """Get a specific signal in this dataset by its name or ID."""
        if name is None and id is None:
            raise ValueError("Either name or id must be provided.")
        if name is not None and id is not None:
            raise ValueError("Only one of name or id can be provided.")

        if name is not None:
            if name not in self._known_signals:
                self.get_signals()
            id = self._known_signals.get(name)

        if id is None:
            raise ValueError(f"Signal with name {name} not found in dataset with id {self.id}.")

        if id not in self._signals:
            r = self._db.get(f"/stream/{self.datastream.id}/dataset/{self.id}/signal/{id}")
            signal = Signal(dataset=self, **r.json())
            self._signals[signal.id] = signal
            self._known_signals[signal.name] = signal.id

        return self._signals[id]

    def get_signals(self, signal_names: list[str | re.Pattern] | None = None) -> list["Signal"]:

        compiled_filters = [
            re.compile(f"^{re.escape(f)}$") if isinstance(f, str) else f for f in signal_names or []
        ]

        def include(signal_name: str) -> bool:
            if signal_names is None:
                return True
            return any(pattern.match(signal_name) for pattern in compiled_filters)

        if not self._has_all_signals:
            r = self._db.get(f"/stream/{self.datastream.id}/dataset/{self.id}/signals")
            for signal in r.json():
                try:
                    signal_obj = Signal(dataset=self, **signal)
                except ValidationError as e:
                    raise UserWarning(
                        f"Failed to parse signal with id {signal.get('id')} and name {signal.get('name')}. Skipping. Error: {e}"
                    )
                self._signals[signal_obj.id] = signal_obj
                self._known_signals[signal_obj.name] = signal_obj.id
            self._has_all_signals = True

        return [signal for signal in self._signals.values() if include(signal.name)]


class Signal(BaseModel):
    id: int
    name: str
    unit: str | None
    description: str | None
    metadata: dict
    storage_status: str
    cold_bytes: int
    hot_bytes: int | None
    count: int
    stats: dict
    count_value: int
    count_text: int
    time_min: int | None
    time_max: int | None
    parquet_version: int

    dataset: Dataset

    def __init__(self, dataset: Dataset, **kwargs):
        super().__init__(dataset=dataset, **kwargs)

    def get_data(self) -> pd.DataFrame:
        # TODO
        return pd.DataFrame()


class DatasetList(UserList[Dataset]):

    def __init__(self, datasets: Iterable[Dataset]):
        super().__init__(datasets)

    def where_stream(
        self, stream_name: str | None = None, stream_id: int | None = None, plugin: str | None = None
    ) -> "DatasetList":
        results = DatasetList([])
        for dataset in self.data:
            if stream_name is not None and dataset.datastream.name != stream_name:
                continue
            if stream_id is not None and dataset.datastream_id != stream_id:
                continue
            dataset_plugin = dataset.datastream.plugin
            if plugin is not None and dataset_plugin is not None and dataset_plugin.lower() != plugin.lower():
                continue
            results.append(dataset)
        return results

    def where_metadata(
        self, metadata: dict[str, int | str | Iterable[int | str]] | None = None
    ) -> "DatasetList":
        results = DatasetList([])
        cleaned_metadata = {k: [v] if not isinstance(v, list) else v for k, v in (metadata or {}).items()}
        for dataset in self.data:
            if any(dataset.metadata.get(field) not in values for field, values in cleaned_metadata.items()):
                continue
            results.append(dataset)
        return results

    def where_dataset(
        self,
        stat: Literal[
            "created_at",
            "created_by",
            "import_status",
            "import_progress",
            "import_time",
            "cold_bytes",
            "hot_bytes",
            "n_datapoints",
            "n_signals",
            "timestamp_start",
            "timestamp_stop",
        ],
        greater_than: float | None = None,
        less_than: float | None = None,
        equals: float | str | None = None,
    ) -> "DatasetList":
        results = DatasetList([])
        for dataset in self.data:
            value = getattr(dataset, stat)
            if greater_than is not None and value <= greater_than:
                continue
            if less_than is not None and value >= less_than:
                continue
            if equals is not None and value != equals:
                continue
            results.append(dataset)
        return results

    def where_signal(
        self,
        signal_name: str,
        stat: Literal[
            "cold_bytes",
            "hot_bytes",
            "count",
            "count_value",
            "count_text",
            "time_min",
            "time_max",
            "max",
            "min",
            "sum",
            "mean",
            "frequency",
        ],
        greater_than: float | None = None,
        less_than: float | None = None,
        equals: float | str | None = None,
    ) -> "DatasetList":
        results = DatasetList([])
        for dataset in self.data:
            signal = dataset.get_signal(signal_name)
            if stat in ["max", "min", "sum", "mean", "frequency"]:
                value = signal.stats.get(stat)
            else:
                value = getattr(signal, stat)
            if value is None:
                raise ValueError(f"Stat {stat} not found in {dataset}. ")
            if greater_than is not None and not value > greater_than:
                continue
            if less_than is not None and not value < less_than:
                continue
            if equals is not None and not value == equals:
                continue
            results.append(dataset)
        return results

    def where_predicate(self, predicate: Callable[[Dataset], bool]) -> "DatasetList":
        return DatasetList([d for d in self.data if predicate(d)])

    def get_data(
        self,
        signals: list[str | re.Pattern],
        resampling: dict | None = None,
    ) -> Iterable[tuple[Dataset, pd.DataFrame]]:
        yield pd.DataFrame()  # TODO


class DB:
    def __init__(self, api_token: str, api_url: str = SAAS_URL):
        self.api_url = api_url
        self.api_token = api_token
        self._known_streams: dict[str, int] = {}
        self._streams: dict[int, DataStream] = {}

        bearer_token = f"Bearer {api_token}"
        self.session = requests.Session()
        self.session.headers.update({"Authorization": bearer_token})
        self.session.headers.update({"X-Request-Source": "sdk/python"})

    # User functions #

    def get(self, url: str, *args, **kwargs) -> Response:
        return self.session.get(f"{self.api_url}{url}", *args, **kwargs)

    def post(self, url: str, *args, **kwargs) -> Response:
        return self.session.post(f"{self.api_url}{url}", *args, **kwargs)

    def patch(self, url: str, *args, **kwargs) -> Response:
        return self.session.patch(f"{self.api_url}{url}", *args, **kwargs)

    def delete(self, url: str, *args, **kwargs) -> Response:
        return self.session.delete(f"{self.api_url}{url}", *args, **kwargs)

    def check_connection(self) -> bool:
        msg_fail_connect = "Could not connect to server at {}".format(self.api_url)
        msg_fail_auth = "Could not authenticate with token"

        try:
            # unauthenticated endpoints
            r = self.get("/health")
            validate_response(r, msg_fail_connect, check_status=False)

            # authenticated endpoint
            r = self.get("/streams")
            validate_response(r, msg_fail_auth, check_status=False)

        except ConnectionError:
            raise Exception(msg_fail_connect)

        return True

    def get_streams(self) -> list[DataStream]:
        if len(self._streams) == 0:
            r = self.get("/streams")
            self._streams = {stream["id"]: DataStream(db=self, **stream) for stream in r.json()["streams"]}
        return list(self._streams.values())

    def get_stream(self, stream_key: str | int) -> DataStream:
        stream_id = self._get_stream_id(stream_key)
        return self._streams[stream_id]

    def get_datasets(self, stream_key: str | int | None = None) -> DatasetList:
        if stream_key is not None:
            return self.get_stream(stream_key).get_datasets()
        return DatasetList([dataset for stream in self.get_streams() for dataset in stream.get_datasets()])

    def get_dataset(self, stream_key: str | int, dataset_id: int) -> Dataset:
        stream = self.get_stream(stream_key)
        return stream.get_dataset(dataset_id)

    def get_signals(self, stream_key: str | int, dataset_id: int) -> list[Signal]:
        return self.get_dataset(stream_key, dataset_id).get_signals()

    def get_signal(self, stream_key: str | int, dataset_id: int, signal_name: str) -> Signal:
        return self.get_dataset(stream_key, dataset_id).get_signal(signal_name)

    def push_file(
        self,
        stream_key: str | int,
        file_path: str,
        metadata: dict | None = None,
        file_name: str | None = None,
    ) -> int:
        stream_id = self._get_stream_id(stream_key)

        with open(file_path, "rb") as file:
            files = {"file": file}
            data = {
                "dataset_name": file_name or Path(file_path).name,
                "metadata": json.dumps(metadata or {}),
            }

            r = self.post(f"/stream/{stream_id}/ingest", files=files, data=data)
            r_json = validate_response(r, "File upload failed")

            return r_json["dataset_id"]

    def get_status(self, stream_key: str | int, dataset_id: int) -> dict:
        stream_id = self._get_stream_id(stream_key)
        r = self.post(f"/stream/{stream_id}/datasets/status", json=[dataset_id])
        if r.status_code != 200:
            r.raise_for_status()

        datasets = r.json()
        for dataset in datasets:
            if dataset["dataset_id"] == dataset_id:
                return dataset

        raise Exception(f"No status found for dataset {dataset_id} in stream {stream_key}")

    def download_original(self, stream_key: str | int, dataset_id: int, destination_folder: str = ".") -> Path:
        """
        Download the original file from the dataset to the destination folder.
        """
        stream_id = self._get_stream_id(stream_key)
        response = self.get(f"/stream/{stream_id}/dataset/{dataset_id}/backup")
        validate_response(response, "Download original file failed", check_status=False)
        download_url = response.json()["path"]
        if not download_url.startswith("http"):
            download_url = f"{self.api_url}/download/{download_url}"

        target_path = Path(destination_folder) / parse.urlparse(download_url).path.rsplit("/")[1]
        request.urlretrieve(download_url, target_path)
        return target_path

    def download_signal(
        self, stream_key: str | int, dataset_id: int, signal_id: int, destination_folder: str = "."
    ) -> list[Path]:
        """
        Download the parquet file for a signal from the dataset to the destination folder.
        """
        stream_id = self._get_stream_id(stream_key)
        r = self.get(f"/stream/{stream_id}/dataset/{dataset_id}/signal/{signal_id}/path")
        validate_response(r, "Get parquet path failed", check_status=False)
        dest = Path(destination_folder)
        parquet_paths = []
        for path in r.json()["paths"]:
            dest_path = dest / parse.urlparse(path).path.rsplit("/")[1]
            request.urlretrieve(path, dest_path)
            parquet_paths.append(dest_path)
        return parquet_paths

    def add_dataset(self, stream_key: str | int, dataset_name: str, metadata: dict | None = None) -> int:
        """
        Create a new empty dataset in the specified live stream.
        Returns the ID of the newly created dataset.

        Use `dataset_append` to add data to the dataset and `upsert_signals` to define signals.

        To add datasets from a file to a file stream, use `push_file` instead.
        """
        stream_id = self._get_stream_id(stream_key)
        r = self.post(
            f"/stream/{stream_id}/dataset/add",
            json={"dataset_name": dataset_name, "metadata": metadata or {}},
        )
        r_json = validate_response(r, "Add dataset failed")

        return r_json["dataset_id"]

    def upsert_signals(self, stream_key: str | int, dataset_id: int, signals: list[dict]) -> None:
        """
        Add signals to a dataset or update existing ones.

        Each signal in the `signals` list should be a dictionary with the following keys:
        - `signal`: Name of the signal
        - `unit`: (optional) Unit of the signal
        - `description`: (optional) Description of the signal
        - `[any metadata key]`: (optional) Any metadata value
        """
        stream_id = self._get_stream_id(stream_key)

        r = self.post(f"/stream/{stream_id}/dataset/{dataset_id}/signals", json=signals)
        validate_response(r, "Upsert signals failed")

    def dataset_append(
        self,
        stream_key: str | int,
        dataset_id: int,
        data: pd.DataFrame,
        shape: Optional[Literal["wide", "long"]] = None,
    ) -> None:
        """
        Append new data to an existing dataset.

        `data` is a DataFrame with the following columns. It can be in either "long" or "wide" format. If `shape` is not specified, the format is automatically detected.
        - `"long"` format: Each row represents a single measurement for a single signal at a specific time. The following columns are expected:
            - `time`: Unix timestamp in nanoseconds.
            - `signal`: Name of the signal as a string. Signals not yet present in the dataset are automatically added. Use `upsert_signals` to set units, descriptions and metadata.
            - `value`: (optional) Value of the signal as a float or integer.
            - `value_text`: (optional) Text value of the signal as a string.
            - At least one of the `value` or `value_text` columns must be present.
        - `"wide"` format: Each row represents a single time point with multiple signals as columns. Expects at least a `time` column.


        """
        stream_id = self._get_stream_id(stream_key)

        if self._detect_shape(shape, data) == "wide":
            if COL_TIME not in data.columns:
                raise ValueError("DataFrame must contain a time column")
            table = _wide_to_long(data)
        else:
            if COL_TIME not in data.columns or COL_SIG not in data.columns:
                raise Exception('DataFrame must contain "time" and "signal" columns')
            if not (COL_VAL in data.columns or COL_VAL_TEXT in data.columns):
                raise Exception('DataFrame must contain at least one of "value" or "value_text" columns')
            value = (
                pd.to_numeric(data[COL_VAL], errors="coerce") if COL_VAL in data.columns else pa.nulls(len(data))
            )
            value_text = data[COL_VAL_TEXT] if COL_VAL_TEXT in data.columns else pa.nulls(len(data))
            table = pa.Table.from_arrays([data[COL_TIME], data[COL_SIG], value, value_text], schema=SCHEMA)

        parquet_buffer = BytesIO()
        pq.write_table(table, parquet_buffer)
        parquet_buffer.seek(0)

        # Send as multipart/form-data
        files = {"file": ("data.parquet", parquet_buffer, "application/octet-stream")}

        r = self.post(f"/stream/{stream_id}/dataset/{dataset_id}/append", files=files)
        validate_response(r, "Append data failed")

    def create_stream(
        self,
        name: str,
        description: Optional[str] = None,
        type: Literal["files", "realtime"] = "files",
        layer_shifts: Optional[list[int]] = None,
        datapool: Optional[str] = None,
        plugin: Optional[str] = None,
        plugin_args: Optional[str] = None,
        signal_reduction: Optional[list] = None,
        insight_workspace: Optional[str] = None,
        insight_project: Optional[str] = None,
    ) -> int:
        """
        Create a new datastream.
        """
        r = self.post(
            "/stream",
            json={
                "name": name,
                "description": description,
                "type": type,
                "layer_shifts": layer_shifts,
                "datapool": datapool,
                "plugin": plugin,
                "plugin_args": plugin_args,
                "signal_reduction": signal_reduction,
                "insight_workspace": insight_workspace,
                "insight_project": insight_project,
            },
        )
        r_json = validate_response(r, "Create stream failed")
        return r_json["id"]

    def delete_stream(self, stream_key: str | int) -> None:
        """
        Delete a datastream and all its datasets.

        This is a destructive operation that cannot be undone.
        """
        stream_id = self._get_stream_id(stream_key)
        r = self.post(f"/stream/{stream_id}/delete")
        validate_response(r, "Delete stream failed")

    # Internal functions #

    def _get_stream_id(self, stream_key: str | int) -> int:
        if isinstance(stream_key, int):
            return stream_key

        if stream_key in self._known_streams:
            return self._known_streams[stream_key]

        streams = self.get_streams()
        for stream in streams:
            if stream.name.lower() == stream_key.lower():
                self._known_streams[stream_key] = stream.id
                return stream.id

        available_streams = ", ".join([s.name for s in streams])
        raise Exception(f'Stream "{stream_key}" not found \nAvailable streams: {available_streams}')

    @staticmethod
    def _detect_shape(shape: Optional[Literal["long", "wide"]], df: pd.DataFrame) -> Literal["long", "wide"]:
        if shape is not None:
            return shape

        if "signal" in df.columns and (("value" in df.columns) or ("value_text" in df.columns)):
            return "long"
        else:
            return "wide"


def _wide_to_long(df: pd.DataFrame) -> pa.Table:
    signals = []
    time = pa.array(df[COL_TIME], type=pa.int64())
    for col in df.columns:
        if col == COL_TIME:
            continue
        if (value := _to_numeric(df[col])) is not None:
            (value, value_text) = (value.to_numpy().astype(np.float64), pa.nulls(len(time)))
        else:
            (value, value_text) = (pa.nulls(len(time)), df[col].fillna("").to_numpy().astype(str))
        signals.append(pa.Table.from_arrays([time, [col] * len(time), value, value_text], schema=SCHEMA))
    return pa.concat_tables(signals)


def _to_numeric(col: pd.Series) -> pd.Series | None:
    if pd.api.types.is_numeric_dtype(col.dtype):
        return col
    null_count = col.isnull().sum()
    numeric_col = pd.to_numeric(col, errors="coerce")
    is_numeric = (numeric_col.isnull().sum() - null_count) / max(len(col), 1) < 0.2
    return numeric_col if is_numeric else None
