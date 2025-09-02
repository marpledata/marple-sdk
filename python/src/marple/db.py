import json
from pathlib import Path
from typing import Optional

import requests
from requests import Response

SAAS_URL = "https://db.marpledata.com/api/v1"


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
            if r.status_code != 200:
                raise Exception(msg_fail_connect)

            # authenticated endpoint
            r = self.get("/user/info")
            if r.status_code != 200:
                raise Exception(msg_fail_auth)

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
            if r.status_code != 200:
                r.raise_for_status()

            r_json = r.json()
            if r_json["status"] != "success":
                raise Exception("Upload failed")

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

    # Internal functions #

    def _stream_name_to_id(self, stream_name: str) -> int:
        streams = self.get_streams()["streams"]
        for stream in streams:
            if stream["name"].lower() == stream_name.lower():
                return stream["id"]

        available_streams = ", ".join([s["name"] for s in streams])
        raise Exception(f'Stream "{stream_name}" not found \nAvailable streams: {available_streams}')
