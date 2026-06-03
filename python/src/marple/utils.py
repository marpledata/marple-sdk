import re
from pathlib import Path
from typing import Iterable, Literal
from urllib import parse, request

import marple
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from marple.db.constants import (
    COL_TIME,
    COL_VAL,
    COL_VAL_IDX,
    COL_VAL_TEXT,
    COL_VAL_TEXT_IDX,
)
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def validate_response(response: requests.Response, failure_message: str) -> dict:
    if response.status_code == 400:
        raise ValueError(f"{failure_message}: Bad request. {response.json().get('error', 'Unknown error')}")
    if response.status_code == 403:
        raise ValueError(f"{failure_message}: Invalid token.")
    if response.status_code == 405:
        raise ValueError(f"{failure_message}: Method not allowed.")
    if response.status_code == 500:
        raise ValueError(f"{failure_message}: {response.json().get('error', 'Unknown error')}")
    if response.status_code != 200:
        response.raise_for_status()
    r_json = response.json()
    if isinstance(r_json, dict) and r_json.get("status", "success") not in ["success", "healthy"]:
        raise ValueError(failure_message)
    return r_json


class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, timeout=None, **kwargs):
        self.timeout = timeout
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        if kwargs.get("timeout") is None:
            kwargs["timeout"] = self.timeout
        return super().send(request, **kwargs)


class DBClient:
    DEFAULT_TIMEOUT = (5, 300)

    def __init__(self, api_token: str, api_url: str, datapool: str, cache_folder: str):
        self.api_token = api_token
        self.api_url = api_url
        self.datapool = datapool
        self.cache_folder = cache_folder
        self._signal_map: dict[str, int] | None = None

        self.session = self._create_api_session()
        self.storage_session = self._create_storage_session()
        self.session.headers.update({"Authorization": f"Bearer {self.api_token}"})
        self.session.headers.update({"X-Request-Source": f"sdk/python:{marple.__version__}"})

    def _create_api_session(self) -> requests.Session:
        retry = Retry(
            total=5,
            connect=5,
            read=2,
            status=2,
            backoff_factor=0.5,
            status_forcelist=(429, 502, 503, 504),
            allowed_methods=frozenset({"GET", "HEAD", "OPTIONS", "DELETE"}),
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        return self._create_session(retry)

    def _create_storage_session(self) -> requests.Session:
        retry = Retry(
            total=5,
            connect=5,
            read=5,
            status=5,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"PUT", "GET", "HEAD"}),
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        return self._create_session(retry)

    def _create_session(self, retry: Retry) -> requests.Session:
        session = requests.Session()
        adapter = TimeoutHTTPAdapter(max_retries=retry, timeout=self.DEFAULT_TIMEOUT)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def get(self, url: str, *args, **kwargs) -> requests.Response:
        return self.session.get(f"{self.api_url}{url}", *args, **kwargs)

    def post(self, url: str, *args, **kwargs) -> requests.Response:
        return self.session.post(f"{self.api_url}{url}", *args, **kwargs)

    def patch(self, url: str, *args, **kwargs) -> requests.Response:
        return self.session.patch(f"{self.api_url}{url}", *args, **kwargs)

    def delete(self, url: str, *args, **kwargs) -> requests.Response:
        return self.session.delete(f"{self.api_url}{url}", *args, **kwargs)

    def get_signal_map(self) -> dict[str, int]:
        if self._signal_map is None:
            r = self.get(f"/datapool/{self.datapool}/signal_map")
            self._signal_map = validate_response(r, "Get signals failed")
        return self._signal_map

    def find_matching_signals(self, signals: Iterable[str | re.Pattern]) -> dict[str, int]:
        all_signals = self.get_signal_map()
        matching = dict()
        for pattern in signals:
            if isinstance(pattern, str) and pattern in all_signals:
                matching[pattern] = all_signals[pattern]
            elif isinstance(pattern, re.Pattern):
                for name, id in all_signals.items():
                    if pattern.search(name):
                        matching[name] = id
        return matching

    def cache_parquet(self, dataset_id: int, signal_id: int, refresh_cache: bool = False) -> Path:
        """
        Download the parquet files for this signal to a local cache folder and return the folder path.
        """
        signal_cache = Path(f"{self.cache_folder}/{self.datapool}/dataset={dataset_id}/signal={signal_id}")
        if not signal_cache.exists() or refresh_cache:
            signal_cache.mkdir(parents=True, exist_ok=True)
            for file in signal_cache.iterdir():
                file.unlink(missing_ok=True)
            r = self.get(f"/datapool/{self.datapool}/dataset/{dataset_id}/signal/{signal_id}/data")
            for path in validate_response(r, "Get parquet path failed"):
                url = parse.urlparse(path)
                request.urlretrieve(url.geturl(), signal_cache / url.path.rsplit("/")[-1])
        return signal_cache

    def list_parquet_files(self, dataset_id: int, signal_id: int, refresh_cache: bool = False) -> list[Path]:
        """
        Get the list of parquet files for this signal, downloading them to the local cache if necessary.

        Args:
            refresh_cache: If True, re-download the parquet files even if they already exist in the cache.
        """
        parquet_folder = self.cache_parquet(dataset_id, signal_id, refresh_cache)
        return [parquet_folder / file.name for file in parquet_folder.iterdir()]

    def get_dataframe(
        self,
        dataset_id: int,
        signal_id: int,
        dtype: Literal["numeric", "text"] | None = None,
        refresh_cache: bool = False,
    ) -> pd.DataFrame:
        """
        Get this signal's raw data as a pandas DataFrame.

        The DataFrame contains two columns: `'time'` and `'value'`.
        The `datatype` parameter determines which data to use in the `value` column.
        """
        if dtype is None:
            dtype = self._infer_dtype(dataset_id, signal_id)
        schema = pa.schema(
            [
                pa.field(COL_TIME, pa.int64()),
                pa.field(COL_VAL, pa.float64()) if dtype == "numeric" else pa.field(COL_VAL_TEXT, pa.string()),
            ]
        )
        df = pd.read_parquet(self.cache_parquet(dataset_id, signal_id, refresh_cache), engine="pyarrow", schema=schema)
        df = df.rename(columns={COL_VAL_TEXT: COL_VAL}).set_index(COL_TIME)
        if df.index.min() > 1e17:
            df.index = pd.to_datetime(df.index, unit="ns")
        else:
            df.index = pd.to_timedelta(df.index, unit="ns")
        return df

    def _infer_dtype(self, dataset_id: int, signal_id: int) -> Literal["numeric", "text"]:
        count_value, count_text = 0, 0
        for file in self.list_parquet_files(dataset_id, signal_id):
            meta = pq.read_metadata(file)
            for idx in range(meta.num_row_groups):
                rg = meta.row_group(idx)
                count_value += rg.column(COL_VAL_IDX).statistics.num_values
                count_text += rg.column(COL_VAL_TEXT_IDX).statistics.num_values
        return "numeric" if count_value >= count_text else "text"
