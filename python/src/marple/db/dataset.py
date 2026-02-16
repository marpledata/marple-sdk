import re
import time
import warnings
from collections import UserList
from pathlib import Path
from typing import Callable, Iterable, Literal, Optional, Sequence
from urllib import parse, request

import pandas as pd
from pandas._typing import AggFuncType, Frequency
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr
from pydantic import ValidationError
import pyarrow.parquet as pq
from marple.db.constants import COL_TIME, COL_VAL

from marple.db.signal import Signal
from marple.utils import DBClient, validate_response

BUSY_STATUSES = [
    "WAITING",
    "IMPORTING",
    "POST_PROCESSING",
    "UPDATING_ICEBERG",
]


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
    def fetch(cls, client: DBClient, dataset_id: int | None = None, dataset_path: str | None = None) -> "Dataset":
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

    def get_signal(self, name: str | None = None, id: int | None = None) -> Optional["Signal"]:
        """Get a specific signal in this dataset by its name or ID."""
        if name is None and id is None:
            raise ValueError("Either name or id must be provided.")
        if name is not None and id is not None:
            raise ValueError("Only one of name or id can be provided.")

        if name is not None:
            id = self._client.get_signal_map().get(name)

        if id is None:
            raise ValueError(f"Signal with name {name} not found in dataset with id {self.id}.")

        if id not in self._signals:
            r = self._client.get(f"/stream/{self.datastream_id}/dataset/{self.id}/signal/{id}")
            try:
                response = validate_response(r, f"Get signal data for signal ID {id} failed")
                signal = Signal(self._client, self.datastream_id, self.id, **response)
            except Exception as e:
                warnings.warn(f"Failed to get signal with id {id} and name {name}: {e}")
                return None
            self._signals[signal.id] = signal

        return self._signals[id]

    def _get_all_signals(self) -> list["Signal"]:
        if self.n_signals is None or len(self._signals) < self.n_signals:
            r = self._client.get(f"/stream/{self.datastream_id}/dataset/{self.id}/signals")
            self._signals.clear()
            for response in validate_response(r, "Failed to get signals"):
                try:
                    signal = Signal(self._client, self.datastream_id, self.id, **response)
                except ValidationError as e:
                    warnings.warn(f"Failed to create signal {response['name']} (id {response['id']}): {e}")
                    continue
                self._signals[signal.id] = signal
        return list(self._signals.values())

    def get_signals(self, signal_names: Iterable[str | re.Pattern] | None = None) -> list["Signal"]:
        """
        Get the signals in this dataset.

        If `signal_names` is provided, only signals with names matching any of the specified strings or regular expression patterns are returned.
        If `signal_names` is None, all signals in the dataset are returned.
        """
        if signal_names is None:
            return self._get_all_signals()
        signals = self._client.find_matching_signals(signal_names)

        r = self._client.get(
            f"/stream/{self.datastream_id}/dataset/{self.id}/signals",
            params={"signal_ids": list(signals.values())},
        )
        signals = []
        for response in validate_response(r, "Failed to get signals by name"):
            try:
                signal = Signal(self._client, self.datastream_id, self.id, **response)
                signals.append(signal)
            except ValidationError as e:
                warnings.warn(f"Failed to create signal {response['name']} (id {response['id']}): {e}")
                continue
            self._signals[signal.id] = signal
        return signals

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
            self._client.find_matching_signals(signals).items(),
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
        return self.fetch(self._client, self.id)

    def wait_for_import(self, timeout: float = 60, force_fetch: bool = False) -> "Dataset":
        """
        Wait for the dataset import to complete.

        If the dataset is still in a busy status (WAITING, IMPORTING, POST_PROCESSING, UPDATING_ICEBERG) after the timeout, a warning is issued and the current dataset information is returned.
        If `force_fetch` is True, the import status is fetched at least once even if the dataset is not in a busy status, to ensure the latest status is returned.
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

    def delete(self) -> None:
        """
        Delete the dataset.

        Warning:
            This is a destructive action that cannot be undone.
        """
        r = self._client.post(f"/stream/{self.datastream_id}/dataset/{self.id}/delete")
        validate_response(r, "Delete dataset failed")


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
                warnings.warn(f"Failed to create dataset with id {value.get('id')} and path {value.get('path')}: {e}")
                continue
            datasets.append(dataset)
        return cls(datasets)

    def where_imported(self) -> "DatasetList":
        """
        Filter datasets that have been successfully imported.
        """
        return self.where(lambda d: d.import_status == "FINISHED")

    def where_metadata(self, metadata: dict[str, int | str | Iterable[int | str]] | None = None) -> "DatasetList":
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
        signal_pairs = list(self.data[0]._client.find_matching_signals(signals).items())
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
