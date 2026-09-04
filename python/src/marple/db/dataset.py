import re
import time
import warnings
from collections import UserList
from enum import StrEnum
from io import BytesIO
from pathlib import Path
from typing import Any, Callable, Iterable, Literal, Optional, Sequence
from urllib import parse, request

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pandas._typing import AggFuncType, Frequency
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, ValidationError

from marple.db.activity import logger
from marple.db.constants import (
    COL_SIG,
    COL_TIME,
    COL_VAL,
    COL_VAL_TEXT,
    MAX_SIGNALS_PER_ADD,
    SCHEMA,
)
from marple.db.script import SandboxJob, SandboxJobStatus, Script
from marple.db.signal import Signal
from marple.db.signal_upload import SignalUpload, run_signal_uploads
from marple.utils import DBClient, validate_response


class ImportStatus(StrEnum):
    """
    Import statuses for a dataset.
    """

    UPLOADING = "UPLOADING"
    WAITING = "WAITING"
    IMPORTING = "IMPORTING"
    POSTPROCESSING = "POSTPROCESSING"
    POSTPROCESSING_FAILED = "POSTPROCESSING_FAILED"
    COOLING = "COOLING"
    COOLING_FAILED = "COOLING_FAILED"
    FINISHED = "FINISHED"
    LIVE = "LIVE"
    FAILED = "FAILED"


STABLE_STATUSES = [
    ImportStatus.FINISHED,
    ImportStatus.LIVE,
    ImportStatus.FAILED,
    ImportStatus.COOLING_FAILED,
    ImportStatus.POSTPROCESSING_FAILED,
]
BUSY_STATUSES = [v for v in ImportStatus if v not in STABLE_STATUSES]

GET_SIGNALS_CHUNK_SIZE = 200


class Dataset(BaseModel):
    """
    Represents a dataset in a Marple DB datastream.

    Args:
        client: DB client used to make API calls.
    """

    model_config = ConfigDict(populate_by_name=True)
    id: int
    datastream_id: int = Field(alias="stream_id")
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
    plugin: str | None
    plugin_args: str | None
    n_datapoints: int | None
    n_signals: int | None
    timestamp_start: int | None
    timestamp_stop: int | None
    import_speed: float | None
    parquet_version: int

    _client: DBClient = PrivateAttr()
    _signals: dict[int, "Signal"] = PrivateAttr(default_factory=dict)

    def __init__(self, client: DBClient, **kwargs):
        kwargs["n_signals"] = kwargs.get("n_signals") or 0
        super().__init__(**kwargs)
        self._client = client

    @classmethod
    def fetch(
        cls, client: DBClient, dataset_id: int | None = None, dataset_path: str | None = None
    ) -> "Dataset":
        """
        Fetch a dataset by its ID or path.

        Args:
            client: DB client used to make API calls.
            dataset_id: The ID of the dataset to fetch.
            dataset_path: The path of the dataset to fetch.
        """
        if dataset_id is None and dataset_path is None:
            raise ValueError("Either dataset_id or dataset_path must be provided.")
        if dataset_id is not None and dataset_path is not None:
            raise ValueError("Only one of dataset_id or dataset_path can be provided.")
        r = client.get(f"/datapool/{client.datapool}/dataset", params={"id": dataset_id, "path": dataset_path})
        return cls(client=client, **validate_response(r, "Get dataset failed"))

    def get_signal(
        self,
        name: str | None = None,
        id: int | None = None,
        *,
        refresh: bool = False,
    ) -> Optional["Signal"]:
        """Get a specific signal in this dataset by its name or ID.

        Args:
            name: Signal name.
            id: Signal ID.
            refresh: If True, refetch from the API even when cached.
        """
        if name is None and id is None:
            raise ValueError("Either name or id must be provided.")
        if name is not None and id is not None:
            raise ValueError("Only one of name or id can be provided.")

        if name is not None:
            id = self._client.get_signal_id(name)

        if id is None:
            raise ValueError(f"Signal with name {name} not found in dataset with id {self.id}.")

        if refresh or id not in self._signals:
            r = self._client.get(f"/stream/{self.datastream_id}/dataset/{self.id}/signal/{id}")
            try:
                response = validate_response(r, f"Get signal data for signal ID {id} failed")
                signal = Signal(self._client, self.datastream_id, self.id, dataset=self, **response)
            except Exception as e:
                warnings.warn(f"Failed to get signal with id {id} and name {name}: {e}")
                return None
            self._signals[signal.id] = signal

        return self._signals[id]

    def _get_all_signals(self) -> list["Signal"]:
        r = self._client.get(f"/stream/{self.datastream_id}/dataset/{self.id}/signals")
        self._signals.clear()
        for response in validate_response(r, "Failed to get signals"):
            try:
                signal = Signal(self._client, self.datastream_id, self.id, dataset=self, **response)
            except ValidationError as e:
                warnings.warn(f"Failed to create signal {response['name']} (id {response['id']}): {e}")
                continue
            self._signals[signal.id] = signal
        self.n_signals = len(self._signals)
        return list(self._signals.values())

    def get_signals(
        self,
        signal_names: Iterable[str | re.Pattern] | None = None,
        *,
        signal_ids: Iterable[int] | None = None,
        refresh: bool = False,
    ) -> list["Signal"]:
        """
        Get signals in this dataset.

        - If neither ``signal_names`` nor ``signal_ids`` is set: all signals.
        - If ``signal_names`` is set: signals matching any of the names / patterns.
        - If ``signal_ids`` is set: signals with those IDs (order preserved).

        Provide only one of ``signal_names`` or ``signal_ids``.

        Args:
            refresh: If True, refetch from the API even when signals are cached.
        """
        if signal_names is not None and signal_ids is not None:
            raise ValueError("Provide only one of signal_names or signal_ids")
        if signal_ids is None and signal_names is None:
            return self._get_all_signals()
        if signal_ids is None:
            assert signal_names is not None
            signal_ids = self._find_signal_ids(signal_names).values()
        return self._get_signals_by_ids(list(signal_ids), refresh=refresh)

    def _find_signal_ids(self, signals: Iterable[str | re.Pattern]) -> dict[str, int]:
        found: dict[str, int] = {}
        exact: list[str] = []
        patterns: list[re.Pattern] = []
        for signal in signals:
            (exact if isinstance(signal, str) else patterns).append(signal)  # type: ignore [arg-type]

        if patterns:
            # Only look up the signal map in the regex case.
            for name, sig_id in self._client.get_signal_map().items():
                if any(pattern.search(name) for pattern in patterns):
                    found[name] = sig_id

        if self.n_signals is not None and self.n_signals < 100:
            self._get_all_signals()

        cached = {s.name: s.id for s in self._signals.values()}
        for name in exact:
            if (signal_id := cached.get(name)) is not None:
                found[name] = signal_id
            elif (signal_id := self._client.get_signal_id(name)) is not None:
                found[name] = signal_id

        return found

    def _get_signals_by_ids(self, signal_ids: list[int], *, refresh: bool = False) -> list["Signal"]:
        if not signal_ids:
            return []

        to_refresh = list(set(signal_ids) if refresh else (set(signal_ids) - set(self._signals.keys())))
        if not to_refresh:
            return [self._signals[i] for i in signal_ids]

        for start in range(0, len(to_refresh), GET_SIGNALS_CHUNK_SIZE):
            chunk = list(to_refresh[start : start + GET_SIGNALS_CHUNK_SIZE])
            r = self._client.get(
                f"/stream/{self.datastream_id}/dataset/{self.id}/signals",
                params={"signal_ids": chunk},
            )
            for response in validate_response(r, "Failed to get signals by id"):
                try:
                    signal = Signal(self._client, self.datastream_id, self.id, dataset=self, **response)
                except ValidationError as e:
                    warnings.warn(
                        f"Failed to create signal {response.get('name')} (id {response.get('id')}): {e}"
                    )
                    continue
                self._signals[signal.id] = signal

        return [self._signals[i] for i in signal_ids if i in self._signals]

    def get_data(
        self,
        signals: Iterable[str | re.Pattern],
        resample_rule: Optional[Frequency] = None,
        resample_aggregate: AggFuncType = "mean",
        dtype: Literal["numeric", "text"] | None = None,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Build a single DataFrame for multiple signals in this dataset.

        Args:
            signals: Iterable of signal names or regular expression patterns to match.
            resample_rule: Pandas resampling frequency (for example `"1s"`). If `None`,
                data is returned at original resolution.
            resample_aggregate: Aggregation used during resampling (for example `"mean"`,
                `"max"`, or a callable).
            dtype: Data type to read from the parquet files. If `None`, the data type is inferred from the signal data.
            **kwargs: Extra keyword arguments forwarded to `DataFrame.resample()`.

        Returns:
            A pandas DataFrame containing one column per signal, aligned on time.
        """
        return self._get_signals_dataframe(
            self._find_signal_ids(signals).items(),
            resample_rule,
            resample_aggregate,
            dtype,
            **kwargs,
        )

    def _get_signals_dataframe(
        self,
        signals: Iterable[tuple[str, int]],
        resample_rule: Optional[Frequency] = None,
        resample_aggregate: AggFuncType = "mean",
        dtype: Literal["numeric", "text"] | None = None,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Build a single DataFrame for multiple signals in this dataset.

        Args:
            signals: Iterable of `(signal_name, signal_id)` pairs to load.
            resample_rule: Pandas resampling frequency (for example `"1s"`). If `None`,
                data is returned at original resolution.
            resample_aggregate: Aggregation used during resampling (for example `"mean"`,
                `"max"`, or a callable).
            dtype: Data type to read from the parquet files. If `None`, the data type is inferred from the signal data.
            **kwargs: Extra keyword arguments forwarded to `DataFrame.resample()`.

        Returns:
            A pandas DataFrame containing one column per signal, aligned on time.
        """
        df = pd.DataFrame()
        for signal_name, signal_id in signals:
            signal = self._client.get_dataframe(self.id, signal_id, dtype).rename(columns={COL_VAL: signal_name})
            df = df.join(signal, how="outer")
        if resample_rule is not None and not df.empty:
            df = df.resample(resample_rule, **kwargs).agg(resample_aggregate)  # type: ignore
        return df

    def download(self, destination_folder: str = ".") -> Path:
        """
        Download the original file from the dataset to the destination folder.
        """
        response = self._client.get(f"/stream/{self.datastream_id}/dataset/{self.id}/backup")
        download_url = validate_response(response, "Download original file failed")["path"]
        if not download_url.startswith("http"):
            download_url = f"{self._client.api_url}/download/{download_url}"

        target_path = Path(destination_folder) / parse.urlparse(download_url).path.rsplit("/")[1]
        request.urlretrieve(download_url, target_path)
        return target_path

    def update_metadata(self, metadata: dict, overwrite: bool = False) -> "Dataset":
        """
        Update the metadata of a dataset.

        By default, the new metadata is merged with the existing metadata.
        If `overwrite` is True, the existing metadata is replaced with the new metadata.
        """
        new_metadata = metadata if overwrite else {**self.metadata, **metadata}
        r = self._client.post(f"/stream/{self.datastream_id}/dataset/{self.id}/metadata", json=new_metadata)
        validate_response(r, "Update metadata failed")
        suffix = " (overwrite=True)" if overwrite else ""
        logger.debug(f"Updated dataset metadata: {metadata}{suffix}")
        return self.fetch(self._client, self.id)

    def upsert_signals(self, signals: list[dict]) -> None:
        """
        Add signals to this dataset or update existing ones.

        Each signal in the `signals` list should be a dictionary with the following keys:
        - `signal`: Name of the signal
        - `unit`: (optional) Unit of the signal
        - `description`: (optional) Description of the signal
        - `[any metadata key]`: (optional) Any metadata value
        """
        r = self._client.post(f"/stream/{self.datastream_id}/dataset/{self.id}/signals", json=signals)
        validate_response(r, "Upsert signals failed")
        logger.debug(f"Upserted metadata for {len(signals)} signals from dataset {self.path}")

    def add_signal(
        self,
        name: str,
        data: pa.Table | pd.DataFrame | pd.Series | Path | str,
        metadata: dict[str, Any] | None = None,
        overwrite: bool = False,
        priority: Literal["default", "high"] = "default",
    ) -> Signal:
        """
        Upload one signal with data into this dataset (enrichment after import,
        or custom ingest after :meth:`~marple.db.datastream.DataStream.add_dataset`).

        ``data`` must be a DataFrame, Series, Arrow table, or parquet path matching
        :data:`~marple.db.LAKE_ARROW_SCHEMA` (``time`` plus ``value`` and/or
        ``value_text``). A Series, or a DataFrame without a ``time`` column, takes its
        sample times from a ``DatetimeIndex`` or ``TimedeltaIndex``.
        It must also contain ``value`` and/or ``value_text`` columns.

        Returns the new signal immediately after upload completes. Call
        :meth:`Signal.wait_until_available` to wait until the signal is available.

        Args:
            name: Signal name.
            data: Signal samples (DataFrame, Series, Arrow table, or parquet path).
            metadata: Optional signal metadata (for example ``unit``).
            overwrite: If True, replace an existing signal with the same name.
            priority: Import priority (``default`` or ``high``).
        """
        signal_id = self.add_signals(
            [SignalUpload(name=name, data=data, metadata=metadata or {}, priority=priority)],
            overwrite=overwrite,
        )[0]
        signal = self.get_signal(id=signal_id, refresh=True)
        if signal is None:
            raise RuntimeError(f"Failed to fetch signal {name} after upload (id {signal_id})")
        return signal

    def add_signals(
        self,
        signals: Sequence[SignalUpload | dict[str, Any]],
        *,
        overwrite: bool = False,
        concurrency: int = 4,
    ) -> list[int]:
        """
        Upload multiple signals with data into this dataset.

        Each item is a :class:`SignalUpload` or a dict with at least ``name`` and
        ``data``. Returns allocated signal IDs as soon as uploads complete (does
        not wait until signals are available).

        Args:
            signals: Signals to upload (at most ``MAX_SIGNALS_PER_ADD``).
            overwrite: If True, replace existing signals that share a name in this batch.
            concurrency: Parallel storage PUT workers.
        """
        if not signals:
            return []
        if len(signals) > MAX_SIGNALS_PER_ADD:
            raise ValueError(f"Provide at most {MAX_SIGNALS_PER_ADD} signals per call")
        if self.import_status not in (ImportStatus.FINISHED, ImportStatus.POSTPROCESSING):
            raise ValueError(f"Dataset {self.id} is not in a writable state (status: {self.import_status})")

        signal_ids = run_signal_uploads(self, signals, overwrite=overwrite, concurrency=concurrency)
        for signal_id in signal_ids:  # Invalidate new signals from cache
            if signal_id in self._signals:
                del self._signals[signal_id]
        names = [item.name if isinstance(item, SignalUpload) else item["name"] for item in signals]
        logger.debug(f"Added {', '.join(names)} to {self.path} (overwrite={overwrite})")
        return signal_ids

    def append(
        self,
        data: pd.DataFrame,
        shape: Optional[Literal["wide", "long"]] = None,
    ) -> None:
        """
        Append new data to this realtime dataset.

        `data` is a DataFrame with the following columns. It can be in either "long"
        or "wide" format. If `shape` is not specified, the format is automatically
        detected:

        - `"long"` format: Each row represents a single measurement for a single signal
          at a specific time. Expects `time`, `signal`, and at least one of `value` or
          `value_text`.
        - `"wide"` format: Each row represents a single time point with multiple signals
          as columns. Expects at least a `time` column.
        """
        if _detect_shape(shape, data) == "wide":
            if COL_TIME not in data.columns:
                raise ValueError("DataFrame must contain a time column")
            table = _wide_to_long(data)
        else:
            if COL_TIME not in data.columns or COL_SIG not in data.columns:
                raise ValueError(f"DataFrame must contain {COL_TIME} and {COL_SIG} columns")
            if not (COL_VAL in data.columns or COL_VAL_TEXT in data.columns):
                raise ValueError(f"DataFrame must contain at least one of {COL_VAL} or {COL_VAL_TEXT} columns")
            value = (
                pd.to_numeric(data[COL_VAL], errors="coerce") if COL_VAL in data.columns else pa.nulls(len(data))
            )
            value_text = data[COL_VAL_TEXT] if COL_VAL_TEXT in data.columns else pa.nulls(len(data))
            table = pa.Table.from_arrays([data[COL_TIME], data[COL_SIG], value, value_text], schema=SCHEMA)

        parquet_buffer = BytesIO()
        pq.write_table(table, parquet_buffer)
        parquet_buffer.seek(0)

        files = {"file": ("data.parquet", parquet_buffer, "application/octet-stream")}
        r = self._client.post(
            f"/stream/{self.datastream_id}/dataset/{self.id}/append",
            files=files,
            timeout=self._client.STORAGE_TIMEOUT,
        )
        validate_response(r, "Append data failed")
        logger.debug(f"Appended {len(data)} rows to realtime dataset {self.path}")

    def cool(self) -> "Dataset":
        """
        Move all realtime data to cold storage and finalize this realtime dataset.

        Cooling is started asynchronously on the server. Poll completion with
        :meth:`wait_for_import`.

        Only datasets in LIVE or COOLING_FAILED status can be cooled.
        After cooling completes, the dataset no longer accepts appends and
        ``import_status`` becomes FINISHED.

        Returns:
            The current dataset state (typically ``import_status == "COOLING"``).
        """
        if self.import_status not in ("LIVE", "COOLING_FAILED"):
            raise ValueError(f"Dataset {self.id} cannot be cooled (status: {self.import_status})")

        r = self._client.post(f"/stream/{self.datastream_id}/dataset/{self.id}/cool")
        validate_response(r, "Cool dataset failed")
        logger.debug(f"Started cooling dataset {self.path}")
        return self.fetch(self._client, self.id)

    def reingest(self, plugin_args: str | None = None) -> "Dataset":
        """
        Reingest this dataset from its original uploaded file.

        Reingestion is started asynchronously on the server. Poll completion with
        :meth:`wait_for_import`.

        Args:
            plugin_args: Optional plugin arguments for this reingest. If omitted, the
                arguments from the previous ingestion are used.

        Returns:
            The current dataset state after the reingest was started.
        """
        kwargs = {} if plugin_args is None else {"json": {"plugin_args": plugin_args}}
        r = self._client.post(f"/stream/{self.datastream_id}/dataset/{self.id}/reingest", **kwargs)
        validate_response(r, "Reingest dataset failed")
        logger.debug(f"Started reingest of dataset {self.path}")
        return self.fetch(self._client, self.id)

    def wait_for_import(self, timeout: float = 60, force_fetch: bool = False) -> "Dataset":
        """
        Wait for the dataset import or cooling to complete.

        If the dataset is still in a busy status (WAITING, IMPORTING, POST_PROCESSING,
        UPDATING_ICEBERG, COOLING) after the timeout, a warning is issued and the current
        dataset information is returned.
        If `force_fetch` is True, the import status is fetched at least once even if the
        dataset is not in a busy status, to ensure the latest status is returned.
        """
        if not (force_fetch or self.import_status in BUSY_STATUSES):
            return self

        deadline = time.monotonic() + max(timeout, 0.1)  # Ensure we fetch at least once
        while time.monotonic() < deadline:
            r = self._client.post(f"/stream/{self.datastream_id}/datasets/status", json=[self.id])
            status = validate_response(r, "Get import status failed")
            if status[0]["import_status"] not in BUSY_STATUSES:
                return self.fetch(self._client, self.id)
            time.sleep(0.5)
        warnings.warn(f"Import did not finish after {timeout} seconds")
        return self.fetch(self._client, self.id)

    def delete_signal(self, signal_id: int) -> None:
        """
        Delete a single signal from this dataset by ID.

        Warning:
            This is a destructive action that cannot be undone.
        """
        self.delete_signals([signal_id])

    def delete_signals(self, signal_ids: Sequence[int]) -> None:
        """
        Delete one or more signals from this dataset by ID.

        Warning:
            This is a destructive action that cannot be undone.
        """
        to_delete = list(set(signal_ids))
        if not to_delete:
            return
        r = self._client.post(
            f"/stream/{self.datastream_id}/dataset/{self.id}/signals/delete",
            json={"signal_ids": to_delete},
        )
        validate_response(r, "Delete signals failed")
        for signal_id in to_delete:
            self._signals.pop(signal_id, None)
        logger.debug(f"Deleted {len(to_delete)} signals from dataset {self.path}")

    def delete(self) -> None:
        """
        Delete the dataset.

        Warning:
            This is a destructive action that cannot be undone.
        """
        r = self._client.post(f"/stream/{self.datastream_id}/dataset/{self.id}/delete")
        validate_response(r, "Delete dataset failed")
        logger.debug(f"Deleted dataset {self.path}")

    def rerun_processing(self) -> "Dataset":
        """
        Rerun aliasing and processing scripts for this dataset.

        Returns the dataset after rerun processing has been queued.
        Wait for completion with :meth:`wait_for_import`.
        """
        r = self._client.post(
            f"/stream/{self.datastream_id}/processing/datasets",
            json=[self.id],
        )
        validate_response(r, "Rerun processing failed")
        logger.debug(f"Reran processing for dataset {self.path}")
        return self.fetch(self._client, self.id)

    def get_debug_messages(self) -> list[str]:
        """Return debug messages for this dataset's latest ingestion (aliasing and pipeline runs).

        Sandbox output from :meth:`run` is on the job log, not this list.
        """
        r = self._client.get(f"/stream/{self.datastream_id}/dataset/{self.id}/debug")
        return validate_response(r, "Get debug messages failed")

    def run(
        self,
        script: Script | int,
        *,
        source: str | Path | None = None,
        version: int | None = None,
        timeout: float = 180,
    ) -> "Dataset":
        """Run a stored processing script on this dataset.

        Args:
            script: A stored script or its ID.
            source: Optional source text or ``.py`` path to save before running.
            version: Script version ID to run. Defaults to the latest version.
            timeout: Seconds to wait for the sandbox job to finish.

        Warning:
            This is not a dry-run. The script will be executed and the dataset will be modified.
        """
        if isinstance(script, (str, Path)):
            raise TypeError(
                "dataset.run() takes a stored Script (or script id). "
                "Create one with db.create_script(name, source), then dataset.run(script)."
            )
        if source is not None and version is not None:
            raise ValueError("Pass source or version, not both; saving creates a new version")

        if source is not None:
            stored = script if isinstance(script, Script) else Script.fetch(self._client, script)
            stored = stored.update(script=source)
            script_id = stored.id
            version = None
        else:
            script_id = script.id if isinstance(script, Script) else script

        payload: dict[str, Any] = {"script_id": script_id}
        if version is not None:
            payload["script_version"] = version

        r = self._client.post(
            f"/stream/{self.datastream_id}/dataset/{self.id}/sandbox-job",
            json=payload,
        )
        job = SandboxJob.model_validate(validate_response(r, "Run script failed", check_envelope=False))

        deadline = time.monotonic() + max(timeout, 0.1)
        while not job.status.is_terminal():
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Script did not finish after {timeout} seconds (job {job.id}, status={job.status})"
                )
            time.sleep(0.5)
            job = SandboxJob.fetch(self._client, job.id)

        if job.status == SandboxJobStatus.FAILED:
            raise RuntimeError(f"Script failed (job {job.id}): {job.log or 'no log'}")

        logger.debug(f"Ran script {script_id} on dataset {self.path}")
        return self.fetch(self._client, self.id)


class DatasetList(UserList[Dataset]):
    """
    A list-like container for datasets with helper filtering methods.

    Args:
        datasets: Iterable of Dataset objects.
    """

    def __init__(self, datasets: Iterable[Dataset]):
        super().__init__(datasets)

    @classmethod
    def from_dicts(cls, client: DBClient, values: Iterable[dict]) -> "DatasetList":
        datasets = []
        for value in values:
            try:
                dataset = Dataset(client=client, **value)
            except ValidationError as e:
                warnings.warn(
                    f"Failed to create dataset with id {value.get('id')} and path {value.get('path')}: {e}"
                )
                continue
            datasets.append(dataset)
        return cls(datasets)

    def where_imported(self) -> "DatasetList":
        """
        Filter datasets that have been successfully imported.
        """
        return self.where(lambda d: d.import_status == "FINISHED")

    def where_metadata(
        self, metadata: dict[str, int | str | Iterable[int | str]] | None = None
    ) -> "DatasetList":
        """
        Filter datasets by their metadata fields.

        Each key in the `metadata` dictionary corresponds to a metadata field name,
        and the associated value is either a single value or an iterable of values.
        A dataset is included in the results if its metadata field matches any of the specified values for all fields.
        """
        cleaned_metadata = {k: [v] if not isinstance(v, list) else v for k, v in (metadata or {}).items()}

        def predicate(dataset: Dataset) -> bool:
            return all(dataset.metadata.get(field) in values for field, values in cleaned_metadata.items())

        return self.where(predicate)

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
        on_missing: Literal["exclude", "include", "raise"] = "exclude",
    ) -> "DatasetList":
        """
        Filter datasets by their statistics.

        If multiple conditions are provided, a dataset must satisfy all of them to be included in the results.
        The `on_missing` parameter determines how to handle cases where the specified statistic is not found in a dataset:
        - "exclude": The dataset is excluded from the results.
        - "include": The dataset is included in the results.
        - "raise": A ValueError is raised.
        """

        def predicate(dataset: Dataset) -> bool:
            value = getattr(dataset, stat)
            if value is None:
                return self._handle_missing(on_missing)
            if greater_than is not None and value <= greater_than:
                return False
            if less_than is not None and value >= less_than:
                return False
            if equals is not None and value != equals:
                return False
            return True

        return self.where(predicate)

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
        on_missing: Literal["exclude", "include", "raise"] = "exclude",
    ) -> "DatasetList":
        """
        Filter datasets by the statistics of a specific signal.

        The `signal_name` parameter specifies the name of the signal to filter by.
        The `stat` parameter specifies the signal statistic to filter by.
        If multiple conditions (greater_than, less_than, equals) are provided, a dataset must satisfy all of them to be included in the results.
        The `on_missing` parameter determines how to handle cases where the specified signal or statistic is not found in a dataset:
        - "exclude": The dataset is excluded from the results.
        - "include": The dataset is included in the results.
        - "raise": A ValueError is raised.
        """

        def predicate(dataset: Dataset) -> bool:
            signal = dataset.get_signal(signal_name)
            if signal is None:
                return False
            if stat in ["max", "min", "sum", "mean", "frequency"]:
                value = (signal.stats or {}).get(stat)
            else:
                value = getattr(signal, stat)
            if value is None:
                return self._handle_missing(on_missing)
            if greater_than is not None and not value > greater_than:
                return False
            if less_than is not None and not value < less_than:
                return False
            if equals is not None and not value == equals:
                return False
            return True

        return self.where(predicate)

    def where(self, predicate: Callable[[Dataset], bool]) -> "DatasetList":
        """
        Filter datasets using a custom predicate function.

        The `predicate` function takes a `Dataset` object as input and returns `True` if the dataset should be included in the results, or `False` otherwise.
        Returns a new `DatasetList` containing only the datasets for which the predicate function returns `True`.
        """
        return DatasetList([d for d in self.data if predicate(d)])

    @staticmethod
    def _handle_missing(on_missing: Literal["exclude", "include", "raise"]) -> bool:
        if on_missing == "raise":
            raise ValueError("Cannot perform comparison on missing value")
        elif on_missing == "exclude":
            return False
        elif on_missing == "include":
            return True
        else:
            raise ValueError(f"Invalid value for on_missing: {on_missing}")

    def get_data(
        self,
        signals: Iterable[str | re.Pattern],
        resample_rule: None | Frequency = None,
        resample_aggregate: AggFuncType = "mean",
        dtype: Literal["numeric", "text"] | None = None,
        **kwargs,
    ) -> Iterable[tuple[Dataset, pd.DataFrame]]:
        """
        Build a single DataFrame for multiple signals for each dataset in the list.

        Args:
            signals: Iterable of signal names or regular expression patterns to match.
            resample_rule: Pandas resampling frequency (for example `"1s"`). If `None`,
                data is returned at original resolution.
            resample_aggregate: Aggregation used during resampling (for example `"mean"`,
                `"max"`, or a callable).
            dtype: Data type to read from the parquet files. If `None`, the data type is inferred from the signal data.
            **kwargs: Extra keyword arguments forwarded to `DataFrame.resample()`.

        Yields:
            Tuples of `(Dataset, DataFrame)`, where the DataFrame contains one column per signal, aligned on time.
        """
        if len(self.data) == 0:
            return
        # Avoid having to search signals for every individual dataset
        signal_pairs = list(self.data[0]._find_signal_ids(signals).items())
        for dataset in self.data:
            yield dataset, dataset._get_signals_dataframe(
                signals=signal_pairs,
                resample_rule=resample_rule,
                resample_aggregate=resample_aggregate,
                dtype=dtype,
                **kwargs,
            )

    def wait_for_import(self, timeout: float = 60, force_fetch: bool = False) -> "DatasetList":
        """
        Wait for the datasets in this DatasetList to be imported.

        If a dataset is still in a busy status (WAITING, IMPORTING, POST_PROCESSING, UPDATING_ICEBERG) after the timeout, a warning is issued and the current dataset information is returned.
        If `force_fetch` is True, the import status is fetched at least once for each dataset even if they are not in a busy status, to ensure the latest status is returned.
        Returns a new DatasetList with the updated dataset information.
        """

        deadline = time.monotonic() + timeout
        return DatasetList(
            [
                dataset.wait_for_import(timeout=deadline - time.monotonic(), force_fetch=force_fetch)
                for dataset in self.data
            ]
        )

    def to_pandas(self) -> pd.DataFrame:
        """
        Convert the DatasetList to a pandas DataFrame with a row for each dataset and columns for id, path, n_signals, n_datapoints, import_status, and all unique metadata fields.
        """
        metadata_fields: set[str] = set()
        for d in self.data:
            metadata_fields.update(d.metadata.keys())
        sorted_metadata_fields = sorted(metadata_fields)

        table_header = ["id", "path", "n_signals", "n_datapoints", "import_status"] + sorted_metadata_fields
        table_data = [
            [d.id, d.path, d.n_signals, d.n_datapoints, d.import_status]
            + [d.metadata.get(field) for field in sorted_metadata_fields]
            for d in self.data
        ]
        return pd.DataFrame(table_data, columns=table_header)

    def __str__(self) -> str:
        pd.DataFrame().__str__
        df = self.to_pandas()

        df_str = df.to_string(
            max_rows=pd.get_option("display.max_rows"),
            max_cols=pd.get_option("display.max_columns"),
            line_width=pd.get_option("display.width"),
            index=False,
        )
        return f"{df_str}\nDatasetList with {len(self.data)} datasets and {len(df.columns) - 5} unique metadata fields."


def _detect_shape(shape: Optional[Literal["long", "wide"]], df: pd.DataFrame) -> Literal["long", "wide"]:
    if shape is not None:
        return shape

    if "signal" in df.columns and ((COL_VAL in df.columns) or (COL_VAL_TEXT in df.columns)):
        return "long"
    return "wide"


def _wide_to_long(df: pd.DataFrame) -> pa.Table:
    signals = []
    time = pa.array(df[COL_TIME], type=pa.int64())
    for col in df.columns:
        if col == COL_TIME:
            continue
        if (numeric_col := _to_numeric(df[col])) is not None:
            value_arr = numeric_col.to_numpy().astype(np.float64)
            value_text = pa.nulls(len(time))
        else:
            value_arr = pa.nulls(len(time))
            value_text = df[col].fillna("").to_numpy().astype(str)
        signals.append(pa.Table.from_arrays([time, [col] * len(time), value_arr, value_text], schema=SCHEMA))
    return pa.concat_tables(signals)


def _to_numeric(col: pd.Series) -> pd.Series | None:
    if pd.api.types.is_numeric_dtype(col.dtype):
        return col
    null_count = col.isnull().sum()
    numeric_col = pd.to_numeric(col, errors="coerce")
    is_numeric = (numeric_col.isnull().sum() - null_count) / max(len(col), 1) < 0.2
    return numeric_col if is_numeric else None
