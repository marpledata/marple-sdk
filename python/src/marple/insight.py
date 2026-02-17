from pathlib import Path
from typing import Optional
from urllib import request

import requests
from marple.utils import validate_response
from requests import Response

SAAS_URL = "https://insight.marpledata.com/api/v1"


class Insight:
    """
    Client for the Marple Insight API.

    Args:
        api_token: The API token for Marple Insight.
        api_url: The URL of the Marple Insight API.
    """

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
        """
        Check if the connection to the Marple Insight API is working.
         - If the connection is successful, returns True.
         - If the connection fails, raise error message.
        """
        default_error =   f"Could not find Marple Insight at {self.api_url}. Please check if the api_url parameter is correct and try again."
        if not self.api_url.endswith("/api/v1"):
            default_error += " The api_url parameter should end with /api/v1"
        try:
            # unauthenticated endpoints
            r = self.get("/version")
            message = r.json()["message"]
            if not message:
                raise Exception(default_error)

        except (ConnectionError, requests.exceptions.RequestException):
            raise Exception(default_error)
        if r.status_code == 404:
            raise Exception(default_error)
        if r.status_code != 200:
            error_text = f"Unknown error occurred while connecting to Marple Insight at {r.request.url}. Status code: {r.status_code}."
            raise Exception(error_text)
        try:
            version = r.json()["message"]
            assert version is not None
        except Exception:
            error_text = f"Received unexpected response from Marple Insight at {r.request.url}. Please check if the api_url parameter is correct and try again."
            raise Exception(error_text)

        # authenticated endpoint
        r = self.get("/")
        if r.status_code != 200:
            error_text = "Invalid API token. Please check if the api_token parameter is correct and not expired."
            raise Exception(error_text)

        return True


    def get_datasets(self) -> list[dict]:
        """
        Get all datasets in the workspace.
        """
        r = self.post("/sources/search", json={"library_filter": {}})
        return validate_response(r, "Failed to get datasets")["message"]

    def get_dataset(self, dataset_filter: dict) -> dict:
        datasets = self.get_datasets()
        dataset = next((d for d in datasets if d["dataset_filter"] == dataset_filter), None)
        if dataset is None:
            raise ValueError(f"Dataset {dataset_filter} not found")
        return dataset

    def get_dataset_mdb(self, dataset_id: int) -> dict:
        """
        Get a Marple DB dataset. (Marple DB Default)
        """
        datasets = self.get_datasets()
        dataset = next((d for d in datasets if d["dataset_filter"]["dataset"] == dataset_id), None)
        if dataset is None:
            raise ValueError(f"Dataset {dataset_id} not found")
        return dataset

    def get_signals(self, dataset_filter: dict) -> list[dict]:
        """
        Get all signals in a dataset. (Marple DB Default)
        """
        r = self.post("/sources/signals", json={"dataset_filter": dataset_filter})
        return validate_response(r, "Failed to get signals")["message"]["signal_list"]

    def get_signals_mdb(self, dataset_id: int) -> list[dict]:
        """
        Get all signals in a dataset. (Marple DB Default)
        """
        dataset = self.get_dataset_mdb(dataset_id)
        return self.get_signals(dataset["dataset_filter"])

    def export_data(
        self,
        dataset_filter: dict,
        format: str = "mat",
        timestamp_start: Optional[int] = None,
        timestamp_stop: Optional[int] = None,
        signals: Optional[list[str]] = None,
        destination: str = ".",
    ):
        """
        Export a dataset to a file.
        """
        dataset = self.get_dataset(dataset_filter)
        return self._export_data(dataset, format, timestamp_start, timestamp_stop, signals, destination)

    def export_data_mdb(
        self,
        dataset_id: int,
        format: str = "mat",
        timestamp_start: Optional[int] = None,
        timestamp_stop: Optional[int] = None,
        signals: Optional[list[str]] = None,
        destination: str = ".",
    ) -> Path:
        """
        Export a dataset to a file. (Only works for Marple DB datasets)
        """
        dataset = self.get_dataset_mdb(dataset_id)
        return self._export_data(dataset, format, timestamp_start, timestamp_stop, signals, destination)

    def _export_data(
        self,
        dataset: dict,
        format: str = "mat",
        timestamp_start: Optional[int] = None,
        timestamp_stop: Optional[int] = None,
        signals: Optional[list[str]] = None,
        destination: str = ".",
    ) -> Path:
        file_name = f"export.{format}"
        signal_list = self.get_signals(dataset["dataset_filter"])
        if signals is not None:
            signal_list = [signal for signal in signal_list if signal["name"] in signals]

        response = self.post(
            "/export",
            json={
                "dataset_filter": dataset["dataset_filter"],
                "export_format": format,
                "file_name": file_name,
                "signals": signal_list,
                "timestamp_start": (dataset["timestamp_start"] if timestamp_start is None else timestamp_start),
                "timestamp_stop": (dataset["timestamp_stop"] if timestamp_stop is None else timestamp_stop),
            },
        )
        temporary_link = validate_response(response, "Failed to export data")["message"]["download_path"]

        download_url = f"{self.api_url}/download/{temporary_link}"
        target_path = Path(destination) / file_name

        request.urlretrieve(download_url, target_path)
        return target_path
