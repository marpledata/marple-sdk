from pathlib import Path
from typing import Optional, Tuple

import requests
from requests import Response

SAAS_URL = "https://insight.marpledata.com/api/v1"


class Insight:
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
            r = self.get("/version")
            if r.status_code != 200:
                raise Exception(msg_fail_connect)

            # authenticated endpoint
            r = self.get("/")
            if r.status_code != 200:
                raise Exception(msg_fail_auth)

        except ConnectionError:
            raise Exception(msg_fail_connect)

        return True

    def export_mdb(
        self,
        stream_id: int,
        dataset_id: int,
        format: str = "mat",
        timestamp_start: Optional[int] = None,
        timestamp_stop: Optional[int] = None,
        destination: str = ".",
    ):
        t_range = self._get_time_range(stream_id, dataset_id)
        file_name = f"export.{format}"

        response = self.post(
            "/export",
            json={
                "dataset_filter": {"dataset": dataset_id, "stream": stream_id},
                "export_format": format,
                "file_name": file_name,
                "signals": self._get_signals(stream_id, dataset_id),
                "timestamp_start": (timestamp_start if timestamp_start is not None else t_range[0]),
                "timestamp_stop": (timestamp_stop if timestamp_stop is not None else t_range[1]),
            },
        )
        temporary_link = response.json()["message"]["download_path"]

        download_url = f"{self.api_url}/download/{temporary_link}"
        target_path = Path(destination) / file_name

        with requests.get(download_url, stream=True) as r:
            r.raise_for_status()
            with open(target_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=65536):  # 64kB
                    if chunk:  # filter out keep-alive chunks
                        f.write(chunk)

    # Internal functions #

    def _get_signals(self, stream_id: int, dataset_id: int) -> list:
        dataset_filter = {"dataset": dataset_id, "stream": stream_id}
        response = self.post(f"/sources/signals", json={"dataset_filter": dataset_filter})
        return response.json()["message"]["signal_list"]

    def _get_time_range(self, stream_id: int, dataset_id: int) -> Tuple[int, int]:
        response = self.post("/sources/search", json={"library_filter": {"stream": stream_id}})
        datasets = response.json()["message"]
        for dataset in datasets:
            if dataset["dataset_filter"]["dataset"] == dataset_id:
                return (dataset["timestamp_start"], dataset["timestamp_stop"])
        raise Exception(f"No time range found for dataset {dataset_id} in stream {stream_id}")
