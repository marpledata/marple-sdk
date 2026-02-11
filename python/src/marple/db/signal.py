import os
from pathlib import Path
from urllib import parse, request

import pandas as pd
import pyarrow as pa
from pydantic import BaseModel, PrivateAttr

from marple.db.constants import COL_TIME, COL_VAL, COL_VAL_TEXT
from marple.utils import DBClient, validate_response


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
    datastream_id: int
    dataset_id: int

    _cold_paths: list[parse.ParseResult] | None = PrivateAttr(default=None)
    _data_folder: Path = PrivateAttr()
    _client: DBClient = PrivateAttr()

    def __init__(self, client: DBClient, datastream_id: int, dataset_id: int, **kwargs):
        super().__init__(datastream_id=datastream_id, dataset_id=dataset_id, **kwargs)
        self._client = client
        self.datastream_id = datastream_id
        self.dataset_id = dataset_id
        self._data_folder = Path(f"{client.cache_folder}/{client.datapool}/dataset={self.dataset_id}/signal={self.id}")

    def _get_paths(self) -> tuple[list[parse.ParseResult], list[str]]:
        if self._cold_paths is None:
            r = self._session.get(f"/stream/{self.datastream_id}/dataset/{self.dataset_id}/signal/{self.id}/path")
            validate_response(r, "Get parquet path failed")
            self._cold_paths = [parse.urlparse(p) for p in r.json()["paths"]]
            os.makedirs(self._data_folder, exist_ok=True)
            return self._cold_paths, os.listdir(self._data_folder)
        return self._cold_paths, os.listdir(self._data_folder)

    def get_local_parquet_paths(self) -> list[Path]:
        """
        Get the local paths of the parquet files for this signal in the cache folder.
        Returns an empty list if the files have not been downloaded yet.
        """
        if self._data_folder is None:
            return []
        return [self._data_folder / p for p in os.listdir(self._data_folder)]

    def download_data(self) -> Path:
        """
        Download the parquet files for this signal to a local cache folder and return the folder path.
        """
        cold_files, loaded_data = self._get_paths()
        assert self._data_folder is not None
        for cold_file in cold_files:
            if cold_file.path.split("/")[-1] not in loaded_data:
                dest_path = self._data_folder / cold_file.path.split("/")[-1]
                request.urlretrieve(cold_file.geturl(), dest_path)
        return self._data_folder

    def get_data(self, prefer_numeric: bool = True) -> pd.DataFrame:
        """
        Get this signal's raw data as a pandas DataFrame.

        The DataFrame contains two columns: `'time'` and `'value'`.
        If the signal contains both numeric and text data, the `prefer_numeric` flag determines which data to use in the `value` column.
        """
        has_numeric = self.count_value > 0
        has_text = self.count_text > 0
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
        df = pd.read_parquet(self.download_data(), engine="pyarrow", schema=schema)
        df = df.rename(columns={COL_VAL_TEXT: COL_VAL})
        if self.time_min is not None and self.time_min > 1e17:
            df[COL_TIME] = pd.to_datetime(df[COL_TIME], unit="ns")
        else:
            df[COL_TIME] = pd.to_timedelta(df[COL_TIME], unit="ns")
        return df
