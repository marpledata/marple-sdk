import json
from io import BytesIO
from pathlib import Path
from typing import Literal, Optional
from urllib import parse, request

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from requests import Response
from requests.exceptions import ConnectionError
import logging

from marple.db.constants import (
    COL_SIG,
    COL_TIME,
    COL_VAL,
    COL_VAL_TEXT,
    SAAS_URL,
    SCHEMA,
)
from marple.db.dataset import Dataset, DatasetList
from marple.db.datastream import DataStream
from marple.db.signal import Signal
from marple.utils import DBClient, validate_response

__all__ = ["DB", "DataStream", "Dataset", "DatasetList", "Signal", "SCHEMA"]


class DB:
    """
    The DB class is the main entry point for the Marple DB API.
    It provides a high-level interface for interacting with the Marple DB API.
    Parameters:
        api_token: The API token for the Marple DB API.
        api_url: The URL of the Marple DB API.
        datapool: The datapool to use (default: "default").
        cache_folder: The folder to cache the data in (default: "./.mdb_cache").
    """

    _streams: dict[int, DataStream] = {}
    client: DBClient

    def __init__(
        self,
        api_token: str,
        api_url: str = SAAS_URL,
        datapool="default",
        cache_folder: str = "./.mdb_cache",
    ):
        self._streams: dict[int, DataStream] = {}
        self.client = DBClient(api_token, api_url, datapool, cache_folder)

    # Utility functions #

    def get(self, url: str, *args, **kwargs) -> Response:
        return self.client.get(url, *args, **kwargs)

    def post(self, url: str, *args, **kwargs) -> Response:
        return self.client.post(url, *args, **kwargs)

    def patch(self, url: str, *args, **kwargs) -> Response:
        return self.client.patch(url, *args, **kwargs)

    def delete(self, url: str, *args, **kwargs) -> Response:
        return self.client.delete(url, *args, **kwargs)

    def check_connection(self) -> bool:
        try:
            r = self.client.get("/health")
        except ConnectionError:
            error_text = f"Could not connect to Marple DB at {self.client.api_url}. Please check if the api_url parameter is correct and try again."
            logging.error(error_text)
            return False
        if r.status_code == 404:
            error_text = f"Could not find Marple DB at {r.request.url}. Please check if the api_url parameter is correct and try again."
            if not self.client.api_url.endswith("/api/v1"):
                error_text += f" The api_url parameter should end with /api/v1"
            logging.error(error_text)
            return False
        if r.status_code != 200:
            error_text = f"Unknown error occurred while connecting to Marple DB at {r.request.url}. Status code: {r.status_code}."
            logging.error(error_text)
            return False

        r = self.client.get("/streams")
        if r.status_code == 403:
            error_text = f"Invalid API token. Please check if the api_token parameter is correct and not expired."
            logging.error(error_text)
            return False

        self._refresh_stream_cache(r)
        return True

    # Stream functions #

    def get_streams(self) -> list[DataStream]:
        self._refresh_stream_cache()
        return list(self._streams.values())

    def get_stream(self, stream_key: str | int) -> DataStream:
        stream_id = self._get_stream_id(stream_key)
        return self._streams[stream_id]

    def _find_stream(self, stream_key: str | int) -> DataStream | None:
        if isinstance(stream_key, int):
            return self._streams.get(stream_key)
        return next(
            (s for s in self._streams.values() if s.name.lower() == stream_key.lower() or str(s.id) == stream_key),
            None,
        )

    def _get_stream_id(self, stream_key: str | int) -> int:
        s = self._find_stream(stream_key)
        if s is not None:
            return s.id
        self._refresh_stream_cache()
        s = self._find_stream(stream_key)
        if s is not None:
            return s.id
        raise Exception(
            f"Stream with name or id {stream_key} not found, available streams: {', '.join([s.name for s in self._streams.values()])}"
        )

    def _refresh_stream_cache(self, r: Response | None = None) -> None:
        if r is None:
            r = self.get("/streams")
        validate_response(r, "Failed to fetch streams")
        self._streams = {stream["id"]: DataStream(client=self.client, **stream) for stream in r.json()["streams"]}

    def get_datasets(self, stream_key: str | int | None = None) -> DatasetList:
        if stream_key is not None:
            return self.get_stream(stream_key).get_datasets()
        datasets = validate_response(
            self.get(f"/datapool/{self.client.datapool}/datasets"),
            f"Failed to get datasets for datapool {self.client.datapool}",
        )
        return DatasetList([Dataset(self.client, **dataset) for dataset in datasets])

    def get_dataset(self, dataset_id: int | None = None, dataset_path: str | None = None) -> Dataset:
        r = self.get(f"/datapool/{self.client.datapool}/dataset", params={"id": dataset_id, "path": dataset_path})
        r = validate_response(r, "Get dataset failed")
        return Dataset(self.client, **r)

    def get_signals(self, dataset_id: int | None = None, dataset_path: str | None = None) -> list[Signal]:
        return self.get_dataset(dataset_id, dataset_path).get_signals()

    def get_signal(
        self,
        dataset_id: int | None = None,
        dataset_path: str | None = None,
        signal_name: str | None = None,
        signal_id: int | None = None,
    ) -> Signal | None:
        return self.get_dataset(dataset_id, dataset_path).get_signal(signal_name, signal_id)

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
            if stream_id in self._streams:
                self._streams = {}
                self._known_streams = {}

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
        stream = self.get_stream(stream_key)
        response = self.get(f"/stream/{stream.id}/dataset/{dataset_id}/backup")
        validate_response(response, "Download original file failed")
        download_url = response.json()["path"]
        if not download_url.startswith("http"):
            download_url = f"{self.client.api_url}/download/{download_url}"

        target_path = Path(destination_folder) / parse.urlparse(download_url).path.rsplit("/")[1]
        request.urlretrieve(download_url, target_path)
        return target_path

    def download_signal(
        self,
        dataset_id: int | None = None,
        dataset_path: str | None = None,
        signal_id: int | None = None,
        signal_name: str | None = None,
    ) -> list[Path]:
        """
        Download the parquet file for a signal from the dataset to the destination folder.
        """
        signal = self.get_signal(
            dataset_id,
            dataset_path,
            signal_name,
            signal_id,
        )
        if signal is None:
            raise Exception("Signal not found")
        signal.download_data()
        return signal.get_local_parquet_paths()

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

    def delete_dataset(self, dataset_id: int | None, dataset_path: str | None):
        """
        Delete a dataset by its ID. This is a destructive operation that cannot be undone.
        """
        dataset = self.get_dataset(dataset_id, dataset_path)
        r = self.post(f"/stream/{dataset.datastream_id}/dataset/{dataset.id}/delete")
        validate_response(r, "Delete dataset failed")

    def update_metadata(
        self, dataset_id: int | None, dataset_path: str | None, metadata: dict, overwrite: bool = False
    ) -> None:
        """
        Update the metadata of a dataset.

        By default, the new metadata is merged with the existing metadata.
        If `overwrite` is True, the existing metadata is replaced with the new metadata.
        """
        dataset = self.get_dataset(dataset_id, dataset_path)
        new_metadata = metadata if overwrite else {**dataset.metadata, **metadata}
        self.post(f"/stream/{dataset.datastream_id}/dataset/{dataset.id}/metadata", json=new_metadata)

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
                raise Exception(f"DataFrame must contain {COL_TIME} and {COL_SIG} columns")
            if not (COL_VAL in data.columns or COL_VAL_TEXT in data.columns):
                raise Exception(f"DataFrame must contain at least one of {COL_VAL} or {COL_VAL_TEXT} columns")
            value = pd.to_numeric(data[COL_VAL], errors="coerce") if COL_VAL in data.columns else pa.nulls(len(data))
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

    @staticmethod
    def _detect_shape(shape: Optional[Literal["long", "wide"]], df: pd.DataFrame) -> Literal["long", "wide"]:
        if shape is not None:
            return shape

        if "signal" in df.columns and ((COL_VAL in df.columns) or (COL_VAL_TEXT in df.columns)):
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
