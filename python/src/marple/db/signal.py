import os
from pathlib import Path
from typing import Literal
from urllib import parse, request

import pandas as pd
import pyarrow as pa
from pydantic import BaseModel, PrivateAttr

from marple.db.constants import COL_TIME, COL_VAL, COL_VAL_TEXT
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

    _cache_folder: Path = PrivateAttr()
    _client: DBClient = PrivateAttr()

    def __init__(self, client: DBClient, datastream_id: int, dataset_id: int, **kwargs):
        super().__init__(datastream_id=datastream_id, dataset_id=dataset_id, **kwargs)
        self._client = client
        self.datastream_id = datastream_id
        self.dataset_id = dataset_id
        self._cache_folder = Path(f"{client.cache_folder}/{client.datapool}/dataset={self.dataset_id}/signal={self.id}")

    def download(self, refresh_cache: bool = False) -> Path:
        """
        Download the parquet files for this signal to a local cache folder and return the folder path.
        """
        if not self._cache_folder.exists() or refresh_cache:
            self._cache_folder.mkdir(parents=True, exist_ok=True)
            for file in self._cache_folder.iterdir():
                file.unlink(missing_ok=True)
            r = self._client.get(f"/datapool/{self._client.datapool}/dataset/{self.dataset_id}/signal/{self.id}/data")
            for path in validate_response(r, "Get parquet data failed"):
                url = parse.urlparse(path)
                request.urlretrieve(url.geturl(), self._cache_folder / url.path.rsplit("/")[-1])
        return self._cache_folder

    def get_parquet_files(self, refresh_cache: bool = False) -> list[Path]:
        """
        Get the list of parquet files for this signal, downloading them to the local cache if necessary.

        Args:
            refresh_cache: If True, re-download the parquet files even if they already exist in the cache.
        """
        parquet_folder = self.download(refresh_cache)
        return [parquet_folder / file.name for file in parquet_folder.iterdir()]

    def get_data(self, prefer_numeric: bool = True, refresh_cache: bool = False) -> pd.DataFrame:
        """
        Get this signal's raw data as a pandas DataFrame.

        The DataFrame contains two columns: `'time'` and `'value'`.
        If the signal contains both numeric and text data, the `prefer_numeric` flag determines which data to use in the `value` column.
        """
        has_numeric = (self.count_value or 0) > 0
        has_text = (self.count_text or 0) > 0
        if has_numeric != has_text:
            use_numeric = has_numeric
        else:
            use_numeric = prefer_numeric

        schema = pa.schema(
            [
                pa.field(COL_TIME, pa.int64()),
                pa.field(COL_VAL, pa.float64()) if use_numeric else pa.field(COL_VAL_TEXT, pa.string()),
            ]
        )
        df = pd.read_parquet(self.download(refresh_cache), engine="pyarrow", schema=schema)
        df = df.rename(columns={COL_VAL_TEXT: COL_VAL})
        if self.time_min is not None and self.time_min > 1e17:
            df[COL_TIME] = pd.to_datetime(df[COL_TIME], unit="ns")
        else:
            df[COL_TIME] = pd.to_timedelta(df[COL_TIME], unit="ns")
        return df
