import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Literal, Optional

import requests
from pydantic import BaseModel, PrivateAttr

from marple.db.dataset import Dataset, DatasetList
from marple.utils import DBClient, validate_response


class DataStream(BaseModel):
    """
    Represents a Marple DB datastream.

    Args:
        client: DB client used to make API calls.
    """

    type: Literal["files", "realtime"]
    id: int
    name: str
    description: str | None
    datapool: str
    layer_shifts: list[int]
    version_id: int
    insight_workspace: Optional[str] = None
    insight_project: Optional[str] = None

    # Stats
    created_at: float
    last_updated: float
    last_ingested: Optional[float] = None
    n_datasets: Optional[int] = None
    n_datapoints: Optional[int] = None
    cold_bytes: Optional[int] = None
    hot_bytes: Optional[int] = None

    plugin: Optional[str] = None
    plugin_args: Optional[str] = None
    signal_reduction: Optional[list] = None

    _client = PrivateAttr()

    def __init__(self, client: DBClient, **kwargs):
        super().__init__(**kwargs)
        self._client = client

    def get_dataset(self, id: int | None = None, path: str | None = None) -> "Dataset":
        """Get a single dataset in the datastream by ID or path."""
        return Dataset.fetch(self._client, id, path)

    def get_datasets(self) -> "DatasetList":
        """Get all datasets in this datastream."""
        r = self._client.get(f"/stream/{self.id}/datasets")
        return DatasetList.from_dicts(self._client, validate_response(r, "Get datasets failed"))

    def push_file(
        self,
        file_path: str,
        metadata: dict | None = None,
        file_name: str | None = None,
        concurrency: int = 4,
    ) -> Dataset:
        """
        Push a file to the datastream. The file will be ingested as a new dataset.

        Args:
            file_path: The path to the file to push.
            metadata: Optional metadata to attach to the dataset.
            file_name: Optional name for the dataset. If not provided, the file name will be used.
            concurrency: Maximum number of concurrent part uploads for multipart uploads.
        """
        path = Path(file_path)
        file_size = path.stat().st_size
        dataset_name = file_name or path.name

        init = self._init_ingestion(dataset_name, file_size, metadata or {})
        ingestion_id = init["ingestion_id"]
        try:
            if init["mode"] == "single":
                presigned_url = init.get("presigned_url")
                if presigned_url is None:
                    raise ValueError("Single upload mode without presigned_url")
                self._put_single(presigned_url, path, file_size)
            elif init["mode"] == "multipart":
                part_size = init.get("part_size")
                if part_size is None:
                    raise ValueError("Multipart upload mode without part_size")
                self._upload_multipart(ingestion_id, path, file_size, part_size, max(concurrency, 1))
            else:
                raise ValueError(f"Unknown upload mode: {init['mode']}")

            self._complete_upload(ingestion_id)
        except BaseException:
            self._abort_upload(ingestion_id)
            raise

        return self.get_dataset(init["dataset_id"])

    def _init_ingestion(self, dataset_name: str, file_size: int, metadata: dict) -> dict:
        r = self._client.post(
            "/ingestion",
            json={
                "stream_id": self.id,
                "dataset_name": dataset_name,
                "file_size": file_size,
                "metadata": metadata,
            },
        )
        return validate_response(r, "Initialize ingestion failed")

    def _get_part_urls(self, ingestion_id: int, start_part: int, count: int) -> dict:
        r = self._client.get(
            f"/ingestion/{ingestion_id}/upload/part-urls",
            params={"start_part": start_part, "count": count},
        )
        return validate_response(r, "Get upload part URLs failed")

    def _complete_upload(self, ingestion_id: int) -> None:
        r = self._client.post(f"/ingestion/{ingestion_id}/upload/complete")
        validate_response(r, "Complete upload failed")

    def _abort_upload(self, ingestion_id: int) -> None:
        try:
            r = self._client.post(f"/ingestion/{ingestion_id}/abort")
            validate_response(r, "Abort upload failed")
        except Exception as e:
            warnings.warn(f"Failed to abort ingestion {ingestion_id}: {e}")

    @staticmethod
    def _put_single(url: str, path: Path, file_size: int) -> None:
        with path.open("rb") as file:
            response = requests.put(url, data=file, headers={"Content-Length": str(file_size)})
        DataStream._validate_storage_response(response, "Storage PUT failed")

    def _upload_multipart(self, ingestion_id: int, path: Path, total_size: int, part_size: int, concurrency: int) -> None:
        if part_size <= 0:
            raise ValueError("Multipart upload part_size must be positive")

        total_parts = (total_size + part_size - 1) // part_size
        next_part = 1
        batch_size = max(concurrency, 32)

        while next_part is not None and next_part <= total_parts:
            urls = self._get_part_urls(ingestion_id, next_part, batch_size)
            parts = urls.get("parts", [])
            if not parts:
                raise RuntimeError("Server returned no multipart upload URLs")

            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                futures = [
                    executor.submit(
                        self._put_part,
                        path,
                        part_size,
                        total_size,
                        part["part_number"],
                        part["url"],
                    )
                    for part in parts
                ]
                for future in as_completed(futures):
                    future.result()

            next_part = urls.get("next_part")

    @staticmethod
    def _put_part(path: Path, part_size: int, total_size: int, part_number: int, url: str) -> None:
        offset = (part_number - 1) * part_size
        if offset >= total_size:
            raise ValueError(f"Part {part_number} offset is outside the file")

        part_len = min(part_size, total_size - offset)
        with path.open("rb") as file:
            file.seek(offset)
            chunk = file.read(part_len)

        response = requests.put(url, data=chunk, headers={"Content-Length": str(part_len)})
        DataStream._validate_storage_response(response, f"Part {part_number} storage PUT failed")

    @staticmethod
    def _validate_storage_response(response: requests.Response, failure_message: str) -> None:
        if not response.ok:
            raise RuntimeError(f"{failure_message}: status {response.status_code}: {response.text}")

    def delete(self) -> None:
        """
        Delete the datastream.

        Warning:
            This is a destructive action that cannot be undone and will delete all datasets in the datastream.
        """
        r = self._client.post(f"/stream/{self.id}/delete")
        validate_response(r, "Delete stream failed")
