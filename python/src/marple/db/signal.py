import time
import warnings
from pathlib import Path
from typing import Literal

import pandas as pd
from pydantic import BaseModel, PrivateAttr

from marple.utils import DBClient, validate_response


class Signal(BaseModel):
    """
    Represents a signal within a dataset.

    Args:
        client: DB client used to make API calls.
        datastream_id: ID of the parent datastream.
        dataset_id: ID of the parent dataset.
    """

    id: int
    name: str
    unit: str | None
    description: str | None
    metadata: dict
    storage_status: Literal["FROZEN_TO_COLD", "COLD", "COLD_TO_HOT", "HOT"]
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

    def __init__(self, client: DBClient, datastream_id: int, dataset_id: int, **kwargs):
        super().__init__(datastream_id=datastream_id, dataset_id=dataset_id, **kwargs)
        self._client = client
        self.datastream_id = datastream_id
        self.dataset_id = dataset_id

    def cache_parquet(self, refresh_cache: bool = False) -> Path:
        """
        Download the parquet files for this signal to a local cache folder and return the folder path.
        Args:
            refresh_cache: If True, re-download the parquet files even if they already exist in the cache.

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

    def wait_until_cold(self, timeout: float = 60) -> "Signal":
        """
        Poll until this signal's ``storage_status`` is ``COLD``.

        After :meth:`Dataset.add_signal` / :meth:`Dataset.add_signals`, the signal
        may briefly be hot or transitioning. Wait here before calling
        :meth:`get_data` if you need lake-cold parquet.

        If the timeout elapses, a warning is issued and the latest signal state is
        returned (status may still be non-cold).
        """
        deadline = time.monotonic() + max(timeout, 0.1)
        while True:
            r = self._client.get(f"/stream/{self.datastream_id}/dataset/{self.dataset_id}/signal/{self.id}")
            data = validate_response(r, f"Get signal {self.id} failed")
            fresh = Signal(self._client, self.datastream_id, self.dataset_id, **data)
            if fresh.storage_status == "COLD":
                return fresh
            if time.monotonic() >= deadline:
                warnings.warn(f"Signal {self.id} did not reach COLD after {timeout} seconds")
                return fresh
            time.sleep(0.5)
