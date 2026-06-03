import warnings
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock
from typing import Literal, Optional

import requests
from pydantic import BaseModel, PrivateAttr

from marple.db.dataset import Dataset, DatasetList
from marple.utils import DBClient, validate_response


class IngestionInit(BaseModel):
    dataset_id: int
    ingestion_id: int
    mode: Literal["server", "azure", "single", "multipart"]
    presigned_url: str | None = None
    part_size: int | None = None
    expires_in: int


class PartUrl(BaseModel):
    part_number: int
    url: str


class PartUrlsResponse(BaseModel):
    parts: list[PartUrl]
    expires_in: int
    next_part: int | None


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
        upload_mode: Literal["auto", "server"] = "auto",
    ) -> Dataset:
        """
        Push a file to the datastream. The file will be ingested as a new dataset.

        Args:
            file_path: The path to the file to push.
            metadata: Optional metadata to attach to the dataset.
            file_name: Optional name for the dataset. If not provided, the file name will be used.
            concurrency: Maximum number of concurrent part uploads for multipart uploads.
            upload_mode: Upload mode override. Use "server" to force upload through the Marple DB API server.
        """
        if upload_mode not in ("auto", "server"):
            raise ValueError("upload_mode must be either 'auto' or 'server'")

        path = Path(file_path)
        file_size = path.stat().st_size
        init = self._init_ingestion(file_name or path.name, file_size, metadata or {})

        try:
            if upload_mode == "server" or init.mode == "server":
                self._upload_server(init, path)
            elif init.mode == "azure":
                self._upload_azure(init, path, max(concurrency, 1))
            elif init.mode == "single":
                self._upload_single(init, path, file_size)
            elif init.mode == "multipart":
                self._upload_multipart(init, path, file_size, max(concurrency, 1))
            else:
                raise ValueError(f"Unknown upload mode: {init.mode}")

            self._complete_upload(init.ingestion_id)
        except BaseException as exc:
            self._abort_upload(init.ingestion_id, str(exc) or type(exc).__name__)
            raise

        return self.get_dataset(init.dataset_id)

    def _init_ingestion(self, dataset_name: str, file_size: int, metadata: dict) -> IngestionInit:
        r = self._client.post(
            "/ingestion",
            json={
                "stream_id": self.id,
                "dataset_name": dataset_name,
                "file_size": file_size,
                "metadata": metadata,
            },
        )
        return IngestionInit(**validate_response(r, "Initialize ingestion failed"))

    def _upload_server(self, init: IngestionInit, path: Path) -> None:
        with path.open("rb") as file:
            files = {"file": (path.name, file, "application/octet-stream")}
            r = self._client.post(f"/ingestion/{init.ingestion_id}/upload/server", files=files)
        validate_response(r, "Server upload failed")

    def _upload_azure(self, init: IngestionInit, path: Path, concurrency: int) -> None:
        from azure.storage.blob import BlobClient

        if init.presigned_url is None:
            raise ValueError("Azure upload mode without presigned_url")
        blob = BlobClient.from_blob_url(init.presigned_url)
        with path.open("rb") as file:
            blob.upload_blob(file, overwrite=True, max_concurrency=concurrency)

    def _upload_single(self, init: IngestionInit, path: Path, file_size: int) -> None:
        if init.presigned_url is None:
            raise ValueError("Single upload mode without presigned_url")
        with path.open("rb") as file:
            response = self._client.storage_session.put(
                init.presigned_url,
                data=file,
                headers={"Content-Length": str(file_size)},
            )
        _validate_storage_response(response, "Storage PUT failed")

    def _upload_multipart(
        self,
        init: IngestionInit,
        path: Path,
        file_size: int,
        concurrency: int,
    ) -> None:
        if init.part_size is None:
            raise ValueError("Multipart upload mode without part_size")
        part_size = init.part_size

        batch_size = max(concurrency, 32)
        parts = self._iter_part_urls(init.ingestion_id, batch_size)
        parts_lock = Lock()

        def next_part() -> PartUrl | None:
            with parts_lock:
                return next(parts, None)

        def upload_worker() -> None:
            with path.open("rb") as file:
                while (part := next_part()) is not None:
                    offset = (part.part_number - 1) * part_size
                    part_len = min(part_size, file_size - offset)
                    file.seek(offset)
                    chunk = file.read(part_len)

                    response = self._client.storage_session.put(
                        part.url,
                        data=chunk,
                        headers={"Content-Length": str(part_len)},
                    )
                    _validate_storage_response(response, f"Part {part.part_number} storage PUT failed")

        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(upload_worker) for _ in range(concurrency)]
            for future in as_completed(futures):
                future.result()

    def _iter_part_urls(self, ingestion_id: int, batch_size: int) -> Iterator[PartUrl]:
        next_part: int | None = 1
        while next_part is not None:
            urls = self._get_part_urls(ingestion_id, next_part, batch_size)
            if not urls.parts:
                raise RuntimeError("Server returned no multipart upload URLs")

            yield from urls.parts
            next_part = urls.next_part

    def _get_part_urls(self, ingestion_id: int, start_part: int, count: int) -> PartUrlsResponse:
        r = self._client.get(
            f"/ingestion/{ingestion_id}/upload/part-urls",
            params={"start_part": start_part, "count": count},
        )
        return PartUrlsResponse(**validate_response(r, "Get upload part URLs failed"))

    def _complete_upload(self, ingestion_id: int) -> None:
        r = self._client.post(f"/ingestion/{ingestion_id}/upload/complete")
        validate_response(r, "Complete upload failed")

    def _abort_upload(self, ingestion_id: int, reason: str) -> None:
        try:
            r = self._client.post(f"/ingestion/{ingestion_id}/abort", json={"reason": reason})
            validate_response(r, "Abort upload failed")
        except Exception as e:
            warnings.warn(f"Failed to abort ingestion {ingestion_id}: {e}")

    def delete(self) -> None:
        """
        Delete the datastream.

        Warning:
            This is a destructive action that cannot be undone and will delete all datasets in the datastream.
        """
        r = self._client.post(f"/stream/{self.id}/delete")
        validate_response(r, "Delete stream failed")


def _validate_storage_response(response: requests.Response, failure_message: str) -> None:
    if not response.ok:
        raise RuntimeError(f"{failure_message}: status {response.status_code}: {response.text}")
