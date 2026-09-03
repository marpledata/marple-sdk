import warnings
from functools import wraps
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Optional, Sequence

import pandas as pd
from pydantic import ValidationError
from requests import Response
from requests.exceptions import ConnectionError

from marple.db import sql
from marple.db.constants import LAKE_ARROW_SCHEMA as _LAKE_ARROW_SCHEMA
from marple.db.constants import SAAS_URL
from marple.db.constants import SCHEMA as _SCHEMA
from marple.db.dataset import Dataset, DatasetList
from marple.db.datastream import DataStream
from marple.db.script import SandboxJob, SandboxJobStatus, Script, ScriptVersion
from marple.db.signal import Signal
from marple.db.signal_upload import SignalsAlreadyExistError, SignalUpload
from marple.utils import DBClient, validate_response

if TYPE_CHECKING:
    from trino.dbapi import Connection

SCHEMA = _SCHEMA
"""Arrow schema for :meth:`~marple.db.Dataset.append` (long-format realtime rows)."""

LAKE_ARROW_SCHEMA = _LAKE_ARROW_SCHEMA
"""Arrow schema for :meth:`~marple.db.Dataset.add_signal` / :meth:`~marple.db.Dataset.add_signals`.

Columns: ``time`` (int64 nanoseconds, required) plus ``value`` (float64) and/or
``value_text`` (string). At least one value column is required; the other may be
omitted and is filled with nulls during validation.
"""

__all__ = [
    "DB",
    "DataStream",
    "Dataset",
    "DatasetList",
    "SandboxJob",
    "SandboxJobStatus",
    "Script",
    "ScriptVersion",
    "Signal",
    "SignalUpload",
    "SignalsAlreadyExistError",
    "SCHEMA",
    "LAKE_ARROW_SCHEMA",
]


def deprecated(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        warnings.warn(
            f"The function db.{func.__name__} is deprecated and it is encouraged to use the Datastream, Dataset and Signal classes directly.",
            category=DeprecationWarning,
            stacklevel=2,
        )
        return func(*args, **kwargs)

    return wrapper


class DB:
    """
    The DB class is the main entry point for the Marple DB API.
    It provides a high-level interface for interacting with the Marple DB API.

    Args:
        api_token: The API token for the Marple DB API.
        api_url: The URL of the Marple DB API.
        datapool: The datapool to use (default: "default").
        cache_folder: The folder to cache the data in (default: "./.mdb_cache").
        trino_host: Override for the Trino query host. By default it is derived
            from `api_url` as `query.<api-host>`.
    """

    _streams: dict[int, DataStream] = {}
    client: DBClient

    def __init__(
        self,
        api_token: str,
        api_url: str = SAAS_URL,
        datapool="default",
        cache_folder: str = "./.mdb_cache",
        trino_host: str | None = None,
    ):
        self._streams: dict[int, DataStream] = {}
        self.client = DBClient(api_token, api_url, datapool, cache_folder, trino_host)
        self.check_connection()

    # Utility functions #

    def get(self, url: str, *args, **kwargs) -> Response:
        """
        Send a GET request to the Marple DB API.
        """
        return self.client.get(url, *args, **kwargs)

    def post(self, url: str, *args, **kwargs) -> Response:
        """Send a POST request to the Marple DB API."""
        return self.client.post(url, *args, **kwargs)

    def patch(self, url: str, *args, **kwargs) -> Response:
        """Send a PATCH request to the Marple DB API."""
        return self.client.patch(url, *args, **kwargs)

    def delete(self, url: str, *args, **kwargs) -> Response:
        """
        Send a DELETE request to the Marple DB API.
        """
        return self.client.delete(url, *args, **kwargs)

    def check_connection(self) -> bool:
        """
        Check if the connection to the Marple DB API is working.
         - If the connection is successful, returns True.
         - If the connection fails, raise an error.
        """
        try:
            r = self.client.get("/health")
        except ConnectionError:
            error_text = f"Could not connect to Marple DB at {self.client.api_url}. Please check if the api_url parameter is correct (ends with /api/v1) and try again."
            raise Exception(error_text)
        if r.status_code == 404:
            error_text = f"Could not find Marple DB at {r.request.url}. Please check if the api_url parameter is correct and try again."
            if not self.client.api_url.endswith("/api/v1"):
                error_text += " The api_url parameter should end with /api/v1"
            raise Exception(error_text)
        if r.status_code != 200:
            error_text = f"Unknown error occurred while connecting to Marple DB at {r.request.url}. Status code: {r.status_code}."
            raise Exception(error_text)
        try:
            status = r.json()["status"]
        except Exception:
            error_text = f"Could not connect to Marple DB at {self.client.api_url}. Please check if the api_url parameter is correct (ends with /api/v1) and try again."
            raise Exception(error_text)
        if status != "healthy":
            error_text = f"Could not connect to Marple DB at {self.client.api_url}. Please check if the api_url parameter is correct (ends with /api/v1) and try again."
            raise Exception(error_text)

        r = self.client.get("/streams")
        if r.status_code == 403:
            try:
                detail = r.json().get("detail")
            except Exception:
                detail = None
            raise Exception(
                detail
                or "Invalid API token. Please check if the api_token parameter is correct and not expired."
            )

        self._refresh_stream_cache(r)
        return True

    # Stream functions #

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
    ) -> DataStream:
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
        return self.get_stream(r_json["id"])

    def delete_stream(self, stream_key: str | int) -> None:
        """
        Delete a datastream and all its datasets.

        Warning:
            This is a destructive operation that cannot be undone.
        """
        stream_id = self._get_stream_id(stream_key)
        r = self.post(f"/stream/{stream_id}/delete")
        validate_response(r, "Delete stream failed")

    def get_streams(self) -> list[DataStream]:
        """
        Get a list of all datastreams in the datapool.
        """
        self._refresh_stream_cache()
        return list(self._streams.values())

    def get_stream(self, stream_key: str | int) -> DataStream:
        """
        Get a datastream by its name or ID.

        """
        stream_id = self._get_stream_id(stream_key)
        return self._streams[stream_id]

    def _find_stream(self, stream_key: str | int) -> DataStream | None:
        if isinstance(stream_key, int):
            return self._streams.get(stream_key)
        return next(
            (
                s
                for s in self._streams.values()
                if s.name.lower() == stream_key.lower() or str(s.id) == stream_key
            ),
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

    def rerun_processing(self, stream_key: str | int, dataset_ids: Sequence[int] | None = None) -> None:
        """
        Rerun aliasing and processing scripts for a stream or selected datasets.

        See :meth:`~marple.db.datastream.DataStream.rerun_processing`.
        """
        self.get_stream(stream_key).rerun_processing(dataset_ids)

    def get_scripts(self) -> list[Script]:
        """List processing scripts in this workspace (without source / version history)."""
        r = self.get("/scripts")
        return [
            Script(client=self.client, **script)
            for script in validate_response(r, "Get scripts failed")["scripts"]
        ]

    def get_script(self, script_id: int) -> Script:
        """Get a processing script by ID, including recent versions and source."""
        return Script.fetch(self.client, script_id)

    def run_script(
        self,
        dataset_id: int,
        script: Script | int,
        *,
        source: str | Path | None = None,
        version: int | None = None,
        timeout: float = 180,
    ) -> Dataset:
        """Run a stored processing script on a dataset.

        See :meth:`~marple.db.dataset.Dataset.run`.

        Args:
            dataset_id: The ID of the dataset to run the script on.
            script: A stored script or its ID.
            source: Optional source text or ``.py`` path to save before running.
            version: Script version ID to run. Defaults to the latest version.
            timeout: Seconds to wait for the sandbox job to finish.

        Warning:
            This is not a dry-run. The script will be executed and the dataset will be modified.
        """
        return self.get_dataset(dataset_id).run(script, source=source, version=version, timeout=timeout)

    def create_script(
        self,
        name: str,
        script: str | Path,
        *,
        description: str | None = None,
    ) -> Script:
        """
        Create a processing script.

        Attach it to a stream with :meth:`~marple.db.datastream.DataStream.update`
        (``scripts=`` replaces the full pipeline).

        Args:
            name: The name of the script.
            script: Source text or a path to a file that must define ``process(dataset)``.
            description: The description of the script.
        """
        r = self.post(
            "/script",
            json={
                "name": name,
                "description": description,
                "script": Script.resolve_source(script),
            },
        )
        return Script(client=self.client, **validate_response(r, "Create script failed"))

    def delete_script(self, script_id: int) -> None:
        """
        Delete a processing script.

        Warning:
            This cannot be undone. The script is removed from every stream pipeline.
        """
        r = self.delete(f"/script/{script_id}")
        validate_response(r, "Delete script failed")

    def _refresh_stream_cache(self, r: Response | None = None) -> None:
        if r is None:
            r = self.get("/streams")

        self._streams.clear()
        for stream in validate_response(r, "Failed to fetch streams")["streams"]:
            try:
                self._streams[stream["id"]] = DataStream(client=self.client, **stream)
            except ValidationError as e:
                warnings.warn(f"Failed to create stream {stream['name']} (id {stream['id']}): {e}")
                continue

    def get_datasets(self, stream_key: str | int | None = None) -> DatasetList:
        """
        Get a list of datasets for a given stream key.

        If stream_key is provided, returns the datasets for the specified stream.
        If stream_key is not provided, returns all datasets in the datapool.
        """
        if stream_key is not None:
            return self.get_stream(stream_key).get_datasets()
        r = self.get(f"/datapool/{self.client.datapool}/datasets")
        datasets = validate_response(r, f"Failed to get datasets for datapool {self.client.datapool}")
        return DatasetList.from_dicts(self.client, datasets)

    def get_dataset(self, dataset_id: int | None = None, dataset_path: str | None = None) -> Dataset:
        """
        Get a dataset by its ID or path.
        """
        return Dataset.fetch(self.client, dataset_id, dataset_path)

    def get_signals(self, dataset_id: int | None = None, dataset_path: str | None = None) -> list[Signal]:
        """
        Get all signals for a dataset.
        """
        return self.get_dataset(dataset_id, dataset_path).get_signals()

    def get_signal(
        self,
        dataset_id: int | None = None,
        dataset_path: str | None = None,
        signal_name: str | None = None,
        signal_id: int | None = None,
    ) -> Signal | None:
        """
        Get a signal from a dataset.

        You can specify the signal by its name or ID and the dataset by its ID or path.
        """
        return self.get_dataset(dataset_id, dataset_path).get_signal(signal_name, signal_id)

    # SQL functions #

    @property
    def trino_info(self) -> dict:
        """
        Connection metadata for SQL querying: `host`, `user`, `hot_catalog`,
        `cold_catalog`, and `datapool`.

        Useful for building fully-qualified table names (see `query`). Not
        available on Marple SaaS.
        """
        return sql.trino_params(self.client)

    def connect_trino(self) -> "Connection":
        """
        Open a raw Trino DBAPI connection to Marple DB and return it.

        Use this for full control (server-side cursors, streaming large
        results, passing to other tools). For one-shot queries, prefer `query`.

        Connection details are discovered automatically from the API token and
        URL. SQL querying is not available on Marple SaaS.
        """
        return sql.connect_trino(self.client)

    def query(self, sql_query: str, params: Optional[Sequence[Any]] = None) -> "pd.DataFrame":
        """
        Run a Trino SQL query against Marple DB and return a pandas DataFrame.

        Hot (Postgres metadata) and cold (Iceberg raw data) are queryable as one
        database. Tables must be fully qualified; see `trino_info` for the
        catalog names. Layout:

        - Hot metadata: `<hot_catalog>.public.mdb_<datapool>_dataset`,
          `..._signal`, `..._signal_enum`.
        - Cold raw data: `<cold_catalog>.<datapool>.data` with columns
          `dataset`, `signal`, `time`, `value`, `value_text`.

        The cold `data` table is keyed by dataset/signal **IDs**, not names; join
        `..._signal_enum` (`name` -> `id`) to filter by signal name.

        Pass `params` as a sequence bound to `?` placeholders to parameterize the
        query. SQL querying is not available on Marple SaaS.

        Example:
            info = db.trino_info
            db.query(
                f"SELECT time, value FROM {info['cold_catalog']}.{info['datapool']}.data "
                "WHERE dataset = ? AND signal = ? LIMIT 1000",
                params=[12, 3],
            )

        Full schema and examples: https://docs.marpledata.com/docs/marple-db/querying
        """
        return sql.query(self.client, sql_query, params)

    # Deprecated functions #

    @deprecated
    def push_file(
        self,
        stream_key: str | int,
        file_path: str,
        metadata: dict | None = None,
        file_name: str | None = None,
        overwrite: bool = False,
    ) -> int:
        """
        Push a file to a datastream.
        - `stream_key`: The name or ID of the stream to push the file to.
        - `file_path`: The path to the file to be pushed.
        - `metadata`: (optional) A dictionary of metadata to be associated with the file.
        - `file_name`: (optional) The name of the file to be stored in the stream.
        - `overwrite`: (optional) If true, existing dataset with the same name will be overwritten.

        Note:
            This function is deprecated and it is encouraged to use the `push_file` method in the `DataStream` class directly.
        """
        stream = self.get_stream(stream_key)
        return stream.push_file(file_path, metadata, file_name, overwrite=overwrite).id

    @deprecated
    def get_status(self, stream_key: str | int, dataset_id: int) -> dict:
        """
        Get the status of a dataset in a stream.

        Note:
          This function is deprecated and it is encouraged to use the Dataset class directly.
        """
        stream_id = self._get_stream_id(stream_key)
        r = self.post(f"/stream/{stream_id}/datasets/status", json=[dataset_id])
        datasets = validate_response(r, "Failed to get status for dataset")["datasets"]
        for dataset in datasets:
            if dataset["dataset_id"] == dataset_id:
                return dataset

        raise Exception(f"No status found for dataset {dataset_id} in stream {stream_key}")

    @deprecated
    def download_original(self, stream_key: str | int, dataset_id: int, destination_folder: str = ".") -> Path:
        """
        Download the original file for a dataset to the destination folder.

        Note:
          This function is deprecated and it is encouraged to use the `download` method in the `Dataset` class directly.
        """
        return self.get_dataset(dataset_id).download(destination_folder)

    @deprecated
    def download_signal(
        self,
        dataset_id: int | None = None,
        dataset_path: str | None = None,
        signal_id: int | None = None,
        signal_name: str | None = None,
        refresh_cache: bool = False,
    ) -> list[Path]:
        """
        Download the parquet file for a signal from the dataset to the destination folder.

        Note:
          This function is deprecated and it is encouraged to use the `download` method in the `Signal` class directly.
        """
        signal = self.get_signal(
            dataset_id,
            dataset_path,
            signal_name,
            signal_id,
        )
        if signal is None:
            raise Exception("Signal not found")
        return signal.list_parquet_files(refresh_cache)

    def delete_dataset(self, dataset_id: int | None, dataset_path: str | None):
        """
        Delete a dataset by its ID.

        Warning:
            This is a destructive operation that cannot be undone.
        """
        dataset = self.get_dataset(dataset_id, dataset_path)
        r = self.post(f"/stream/{dataset.datastream_id}/dataset/{dataset.id}/delete")
        validate_response(r, "Delete dataset failed")

    def delete_signals(
        self,
        dataset_id: int | None,
        dataset_path: str | None,
        signal_ids: Sequence[int],
    ) -> None:
        """
        Delete signals from a dataset by ID or path.

        Warning:
            This is a destructive operation that cannot be undone.
        """
        self.get_dataset(dataset_id, dataset_path).delete_signals(signal_ids)

    @deprecated
    def update_metadata(
        self,
        dataset_id: int | None = None,
        dataset_path: str | None = None,
        metadata: dict | None = None,
        overwrite: bool = False,
    ) -> None:
        """
        Update the metadata of a dataset.

        By default, the new metadata is merged with the existing metadata.
        If `overwrite` is True, the existing metadata is replaced with the new metadata.

        Note:
          This function is deprecated and it is encouraged to use the `update_metadata` method in the `Dataset` class directly.
        """
        if metadata is None:
            metadata = {}
        self.get_dataset(dataset_id, dataset_path).update_metadata(metadata, overwrite)

    # Dataset creation and realtime ingest #

    def add_dataset(self, stream_key: str | int, dataset_name: str, metadata: dict | None = None) -> int:
        """
        Create a new empty dataset in the specified stream.

        **Live (realtime) stream**
            - Define signals with :meth:`~marple.db.dataset.Dataset.upsert_signals`
            - Push data with :meth:`~marple.db.dataset.Dataset.append`
            - Call :meth:`~marple.db.dataset.Dataset.cool` when finished to move the
              dataset to cold Parquet/Iceberg storage, then
              :meth:`~marple.db.dataset.Dataset.wait_for_import` until it is `FINISHED`.
        **File (non-live) stream**
            - Default path: use :meth:`push_file` instead of `add_dataset` — upload a
              file (e.g. CSV); Marple parses it and writes it to the data lake.
            - Custom ingestion: create an empty dataset with `add_dataset`, then upload
              signal data with :meth:`~marple.db.dataset.Dataset.add_signal` /
              :meth:`~marple.db.dataset.Dataset.add_signals` straight into the lake.
        """
        return self.get_stream(stream_key).add_dataset(dataset_name, metadata).id

    def upsert_signals(self, stream_key: str | int, dataset_id: int, signals: list[dict]) -> None:
        """
        Add signals to a dataset or update existing ones.

        Each signal in the `signals` list should be a dictionary with the following keys:
        - `signal`: Name of the signal
        - `unit`: (optional) Unit of the signal
        - `description`: (optional) Description of the signal
        - `[any metadata key]`: (optional) Any metadata value
        """
        self.get_dataset(dataset_id=dataset_id).upsert_signals(signals)

    def dataset_append(
        self,
        stream_key: str | int,
        dataset_id: int,
        data: pd.DataFrame,
        shape: Optional[Literal["wide", "long"]] = None,
    ) -> None:
        """
        Append new data to an existing dataset.

        `data` is a DataFrame with the following columns. It can be in either "long" or "wide" format. If `shape` is not specified, the format is automatically detected:

        - `"long"` format: Each row represents a single measurement for a single signal at a specific time. The following columns are expected:

          - `time`: Unix timestamp in nanoseconds.
          - `signal`: Name of the signal as a string. Signals not yet present in the dataset are automatically added. Use `upsert_signals` to set units, descriptions and metadata.
          - `value`: (optional) Value of the signal as a float or integer.
          - `value_text`: (optional) Text value of the signal as a string.
          - At least one of the `value` or `value_text` columns must be present.

        - `"wide"` format: Each row represents a single time point with multiple signals as columns. Expects at least a `time` column.

        """
        self.get_dataset(dataset_id=dataset_id).append(data, shape)

    def dataset_cool(self, stream_key: str | int, dataset_id: int) -> Dataset:
        """
        Move all realtime data to cold storage and finalize the specified realtime dataset.

        Cooling is started asynchronously on the server. Poll completion with
        `Dataset.wait_for_import`.

        Only realtime datasets in LIVE or COOLING_FAILED status can be cooled. After
        cooling completes, the dataset no longer accepts appends and
        `import_status` becomes FINISHED.

        Returns the current dataset state (typically `import_status == "COOLING"`).
        """
        return self.get_dataset(dataset_id=dataset_id).cool()
