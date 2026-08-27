import time
import warnings
from enum import StrEnum
from pathlib import Path
from typing import Literal

import pandas as pd
from pydantic import BaseModel, PrivateAttr

from marple.utils import DBClient, validate_response


class StorageStatus(StrEnum):
    """
    Storage statuses for a signal.
    """

    FROZEN_TO_COLD = "FROZEN_TO_COLD"
    """ Indicates the signal is being loaded into the cold storage."""
    COLD = "COLD"
    """ Indicates the signal is ready for querying. """
    COLD_TO_HOT = "COLD_TO_HOT"
    """ Indicates the signal is being loaded into the hot cache."""
    HOT = "HOT"
    """ Indicates the signal is in the hot cache."""


class Signal(BaseModel):
    """
    Represents a signal within a dataset.

    Args:
        client: DB client used to make API calls.
        datastream_id: ID of the parent datastream.
        dataset_id: ID of the parent dataset.
        dataset: Optional parent dataset; used to keep its signal cache in sync.
    """

    id: int
    name: str
    unit: str | None
    description: str | None
    metadata: dict
    storage_status: StorageStatus
    cold_bytes: int | None
    hot_bytes: int | None
    count: int | None
    stats: dict | None
    count_value: int | None
    count_text: int | None
    time_min: int | None
    time_max: int | None
    parquet_version: int
    datastream_id: int
    dataset_id: int

    _client: DBClient = PrivateAttr()
    _dataset: object | None = PrivateAttr(default=None)

    def __init__(
        self,
        client: DBClient,
        datastream_id: int,
        dataset_id: int,
        dataset: object | None = None,
        **kwargs,
    ):
        super().__init__(datastream_id=datastream_id, dataset_id=dataset_id, **kwargs)
        self._client = client
        self._dataset = dataset
        self.datastream_id = datastream_id
        self.dataset_id = dataset_id

    def cache_parquet(self, refresh_cache: bool = False) -> Path:
        """
        Download the parquet files for this signal to a local cache folder and return the folder path.
        Args:
            refresh_cache: If True, re-download the parquet files even if they already exist in the cache.
                An empty cache folder is always treated as a miss.

        Returns:
            The path to the local cache folder.
        """
        return self._client.cache_parquet(self.dataset_id, self.id, refresh_cache)

    def list_parquet_files(self, refresh_cache: bool = False) -> list[Path]:
        """
        Get the list of parquet files for this signal, downloading them to the local cache if necessary.

        Args:
            refresh_cache: If True, re-download the parquet files even if they already exist in the cache.

        Returns:
            The list of paths to the parquet files in the local cache.
        """
        return self._client.list_parquet_files(self.dataset_id, self.id, refresh_cache)

    def get_data(
        self, dtype: Literal["numeric", "text"] | None = None, refresh_cache: bool = False
    ) -> pd.DataFrame:
        """
        Get this signal's raw data as a pandas DataFrame.

        Args:
            dtype: Data type to read from the parquet files. If `None`, the data type is inferred from the signal data.
            refresh_cache: If True, re-download the parquet files even if they already exist in the cache.

        Returns:
            A pandas DataFrame containing the signal data.
        """
        return self._client.get_dataframe(self.dataset_id, self.id, dtype, refresh_cache)

    def wait_until_available(self, timeout: float = 60) -> "Signal":
        """
        Poll until this signal is available for querying.
        Args:
            timeout: Maximum time to wait for the signal to be available in seconds.
        Returns:
            The signal object when it is available for querying, or a the last state before the timeout.
        """
        deadline_s = time.monotonic() + max(timeout, 0.1)
        while True:
            r = self._client.get(f"/stream/{self.datastream_id}/dataset/{self.dataset_id}/signal/{self.id}")
            data = validate_response(r, f"Get signal {self.id} failed")
            fresh = Signal(
                self._client,
                self.datastream_id,
                self.dataset_id,
                dataset=self._dataset,
                **data,
            )
            if self._dataset is not None:
                self._dataset._signals[fresh.id] = fresh  # type: ignore[attr-defined]
            if fresh.storage_status != StorageStatus.FROZEN_TO_COLD:
                return fresh
            if time.monotonic() >= deadline_s:
                warnings.warn(f"Signal {self.name} did not reach a queryable state after {timeout} seconds")
                return fresh
            time.sleep(0.5)

    def delete(self) -> None:
        """
        Delete this signal from its dataset.

        Warning:
            This is a destructive action that cannot be undone.
        """
        if self._dataset is not None and hasattr(self._dataset, "delete_signal"):
            self._dataset.delete_signal(self.id)  # type: ignore[attr-defined]
