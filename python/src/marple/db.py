import json
from io import BytesIO
from pathlib import Path
from typing import Literal, Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from requests import Response

SAAS_URL = "https://db.marpledata.com/api/v1"

COL_TIME = "time"
COL_SIG = "signal"
COL_VAL = "value"
COL_VAL_TEXT = "value_text"


class DB:
    def __init__(self, api_token: str, api_url: str = SAAS_URL):
        self.api_url = api_url
        self.api_token = api_token

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
            self._validate_response(r, msg_fail_connect, check_status=False)

            # authenticated endpoint
            r = self.get("/user/info")
            self._validate_response(r, msg_fail_auth, check_status=False)

        except ConnectionError:
            raise Exception(msg_fail_connect)

        return True

    def get_streams(self) -> dict:
        r = self.get("/streams")
        return r.json()

    def get_datasets(self, stream_name: str) -> dict:
        stream_id = self._stream_name_to_id(stream_name)
        r = self.get(f"/stream/{stream_id}/datasets")
        return r.json()

    def push_file(self, stream_name: str, file_path: str, metadata: dict = {}, file_name: Optional[str] = None) -> int:
        stream_id = self._stream_name_to_id(stream_name)

        with open(file_path, "rb") as file:
            files = {"file": file}
            data = {
                "dataset_name": file_name or Path(file_path).name,
                "metadata": json.dumps(metadata),
            }

            r = self.post(f"/stream/{stream_id}/ingest", files=files, data=data)
            r_json = self._validate_response(r, "File upload failed")

            return r_json["dataset_id"]

    def get_status(self, stream_name: str, dataset_id: str) -> dict:
        stream_id = self._stream_name_to_id(stream_name)
        r = self.post(f"/stream/{stream_id}/datasets/status", json=[dataset_id])
        if r.status_code != 200:
            r.raise_for_status()

        datasets = r.json()
        for dataset in datasets:
            if dataset["dataset_id"] == dataset_id:
                return dataset

        raise Exception(f"No status found for dataset {dataset_id} in stream {stream_name}")

    def download_original(self, stream_name: str, dataset_id: str, destination: str = ".") -> None:
        stream_id = self._stream_name_to_id(stream_name)
        response = self.get(f"/stream/{stream_id}/dataset/{dataset_id}/backup")
        temporary_link = Path(response.json()["path"])

        download_url = f"{self.api_url}/download/{temporary_link}"
        target_path = Path(destination) / temporary_link.name

        with requests.get(download_url, stream=True) as r:
            r.raise_for_status()
            with open(target_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=65536):  # 64kB
                    if chunk:  # filter out keep-alive chunks
                        f.write(chunk)

    def add_dataset(self, stream_name: str, dataset_name: str, metadata: dict = {}) -> int:
        """
        Create a new empty dataset in the specified stream.
        Returns the ID of the newly created dataset.

        Use `dataset_append` to add data to the dataset and `upsert_signals` to define signals.
        If you already have data in a file, use `push_file` instead.
        """
        stream_id = self._stream_name_to_id(stream_name)
        r = self.post(
            f"/stream/{stream_id}/datasets/add",
            data={
                "dataset_name": dataset_name,
                "metadata": metadata,
            },
        )
        r_json = self._validate_response(r, "Add dataset failed")

        return r_json["dataset_id"]

    def upsert_signals(self, stream_name: str, dataset_id: int, signals: list[dict]) -> None:
        """
        Add signals to a dataset or update existing ones.

        Each signal in the `signals` list should be a dictionary with the following keys:
        - `signal`: Name of the signal
        - `unit`: (optional) Unit of the signal
        - `description`: (optional) Description of the signal
        - [any metadata key]': (optional) Any metadata value
        """
        stream_id = self._stream_name_to_id(stream_name)

        r = self.post(f"/stream/{stream_id}/dataset/{dataset_id}/signals", json=signals)
        r_json = self._validate_response(r, "Upsert signals failed")

    def dataset_append(
        self, stream_name: str, dataset_id: int, data: pd.DataFrame, shape: Optional[Literal["wide", "long"]] = None
    ) -> None:
        """
        Append new data to an existing dataset.

        `data` is a DataFrame with the following columns. It can be in either "long" or "wide" format. If `shape` is not specified, the format is automatically detected.
        - `"long"` format: Each row represents a single measurement for a single signal at a specific time. The following columns are expected:
            - `time`: Unix timestamp in nanoseconds.
            - `signal`: Name of the signal as a string. Signals not yet present in the dataset are automatically added. Use `upsert_signals` to set units, descriptions and metadata.
            - `value`: (optional) Value of the signal as a float or integer.
            - `value_text`: (optional) Text value of the signal as a string.
        - `"wide"` format: Each row represents a single time point with multiple signals as columns. Expects at least a `time` column.


        At least one of the `value` or `value_text` columns must be present.
        """
        stream_id = self._stream_name_to_id(stream_name)

        if self._detect_shape(shape, data) == "wide":
            data = self._wide_to_long(data)

        if "time" not in data.columns or "signal" not in data.columns:
            raise Exception('DataFrame must contain "time" and "signal" columns')
        if "value" not in data.columns and "value_text" not in data.columns:
            raise Exception('DataFrame must contain at least one of "value" or "value_text" columns')
        if "value" in data.columns and not pd.api.types.is_numeric_dtype(data["value"]):
            raise Exception('"value" column must be numeric')
        if "value_text" in data.columns and not pd.api.types.is_string_dtype(data["value_text"]):
            raise Exception('"value_text" column must be string')

        table = pa.Table.from_pandas(data)
        buf = BytesIO()
        pq.write_table(table, buf)
        buf.seek(0)

        # Send as multipart/form-data
        files = {"file": ("data.parquet", buf, "application/octet-stream")}

        r = self.post(f"/stream/{stream_id}/dataset/{dataset_id}/append", files=files)
        self._validate_response(r, "Append data failed")

    def create_stream(
        self,
        name: str,
        description: Optional[str] = None,
        type: Literal["files", "realtime"] = "files",
        layer_shifts: Optional[list[int]] = None,
        datapool: Optional[str] = None,
        plugin: Optional[str] = None,
        plugin_args: Optional[str] = None,
        signal_reduction: Optional[dict] = None,
        insight_workspace: Optional[str] = None,
        insight_project: Optional[str] = None,
    ) -> int:
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
        r_json = self._validate_response(r, "Create stream failed")
        return r_json["id"]

    def delete_stream(self, stream_name: str) -> None:
        """
        Delete a datastream and all its datasets.

        This is a destructive operation that cannot be undone.
        """
        stream_id = self._stream_name_to_id(stream_name)
        r = self.post(f"/stream/{stream_id}/delete")
        self._validate_response(r, "Delete stream failed")

    # Internal functions #

    def _stream_name_to_id(self, stream_name: str) -> int:
        streams = self.get_streams()["streams"]
        for stream in streams:
            if stream["name"].lower() == stream_name.lower():
                return stream["id"]

        available_streams = ", ".join([s["name"] for s in streams])
        raise Exception(f'Stream "{stream_name}" not found \nAvailable streams: {available_streams}')

    @staticmethod
    def _validate_response(response: Response, failure_message: str, check_status: bool = True) -> dict:
        if response.status_code == 400 or response.status_code == 500:
            raise Exception(f"{failure_message}: {response.json().get('error', 'Unknown error')}")
        if response.status_code != 200:
            response.raise_for_status()
        r_json = response.json()
        if check_status and r_json["status"] != "success":
            raise Exception(failure_message)
        return r_json

    @staticmethod
    def _detect_shape(shape: Optional[Literal["long", "wide"]], df: pd.DataFrame) -> Literal["long", "wide"]:
        if shape is not None:
            return shape

        if "signal" in df.columns and (("value" in df.columns) or ("value_text" in df.columns)):
            return "long"
        else:
            return "wide"

    @staticmethod
    def _wide_to_long(
        df: pd.DataFrame,
    ) -> pd.DataFrame:
        numeric_cols = df.select_dtypes(include=np.number).columns
        long_numeric = df.melt(
            id_vars=[COL_TIME],
            value_vars=[c for c in numeric_cols if c != COL_TIME],
            var_name=COL_SIG,
            value_name=COL_VAL,
        )

        text_cols = df.select_dtypes(include="object").columns
        long_text = df.melt(
            id_vars=[COL_TIME],
            value_vars=[c for c in text_cols if c != COL_TIME],
            var_name=COL_SIG,
            value_name=COL_VAL_TEXT,
        )

        long_numeric[COL_VAL_TEXT] = np.nan
        long_text[COL_VAL] = np.nan

        long_df = pd.concat([long_numeric, long_text], ignore_index=True)

        long_df = long_df[[COL_TIME, COL_SIG, COL_VAL, COL_VAL_TEXT]]
        long_df[COL_SIG] = long_df[COL_SIG].astype("string")
        long_df[COL_VAL_TEXT] = long_df[COL_VAL_TEXT].astype("string")
        return long_df
