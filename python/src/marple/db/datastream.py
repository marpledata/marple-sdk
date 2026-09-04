import warnings
from collections.abc import Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, PrivateAttr, field_validator

from marple.db.activity import logger
from marple.db.dataset import Dataset, DatasetList
from marple.utils import (
    OMITTED,
    DBClient,
    Omitted,
    validate_response,
    validate_storage_response,
)


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
    scripts: list[int] = Field(default_factory=list)
    """IDs of processing scripts in pipeline order. Empty if none are attached."""

    _client = PrivateAttr()

    def __init__(self, client: DBClient, **kwargs):
        super().__init__(**kwargs)
        self._client = client

    @field_validator("scripts", mode="before")
    @classmethod
    def _default_scripts(cls, value: object) -> object:
        """Default to empty list if scripts is None."""
        return value or []

    @classmethod
    def fetch(cls, client: DBClient, stream_id: int) -> "DataStream":
        """Fetch a datastream by ID."""
        r = client.get(f"/stream/{stream_id}")
        return cls(client=client, **validate_response(r, "Get stream failed"))

    def refresh(self) -> "DataStream":
        """Return a freshly fetched copy of this datastream."""
        return self.fetch(self._client, self.id)

    def get_dataset(self, id: int | None = None, path: str | None = None) -> "Dataset":
        """Get a single dataset in the datastream by ID or path."""
        return Dataset.fetch(self._client, id, path)

    def get_datasets(self) -> "DatasetList":
        """Get all datasets in this datastream."""
        r = self._client.get(f"/stream/{self.id}/datasets")
        return DatasetList.from_dicts(self._client, validate_response(r, "Get datasets failed"))

    def add_dataset(self, dataset_name: str, metadata: dict | None = None) -> Dataset:
        """
        Create a new empty dataset in the specified stream.

        **Live (realtime) stream**
            - Define signals with :meth:`~marple.db.dataset.Dataset.upsert_signals`
            - Push data with :meth:`~marple.db.dataset.Dataset.append`
            - Call :meth:`~marple.db.dataset.Dataset.cool` when finished to move the
              dataset to cold Parquet/Iceberg storage, then
              :meth:`~marple.db.dataset.Dataset.wait_for_import` until it is `FINISHED`.
        **File (non-live) stream**
            - Default path: use :meth:`push_file` instead of `add_dataset` — upload a
              file (e.g. CSV); Marple parses it and writes it to the data lake.
            - Custom ingestion: create an empty dataset with `add_dataset`, then upload
              signal data with :meth:`~marple.db.dataset.Dataset.add_signal` /
              :meth:`~marple.db.dataset.Dataset.add_signals` straight into the lake.
        """
        r = self._client.post(
            f"/stream/{self.id}/dataset/add",
            json={"dataset_name": dataset_name, "metadata": metadata or {}},
        )
        r_json = validate_response(r, "Add dataset failed")
        logger.debug("Added dataset %s", dataset_name)
        return self.get_dataset(r_json["dataset_id"])

    def push_file(
        self,
        file_path: str,
        metadata: dict | None = None,
        file_name: str | None = None,
        concurrency: int = 4,
        upload_mode: Literal["auto", "server"] = "auto",
        overwrite: bool = False,
        plugin_args: str | None = None,
    ) -> Dataset:
        """
        Push a file to the datastream. The file will be ingested as a new dataset.

        Args:
            file_path: The path to the file to push.
            metadata: Optional metadata to attach to the dataset.
            file_name: Optional name for the dataset. If not provided, the file name will be used.
            concurrency: Maximum number of concurrent part uploads for multipart uploads.
            upload_mode: Upload mode override. Use "server" to force upload through the Marple DB API server.
            overwrite: If true, existing dataset with the same name will be overwritten.
            plugin_args: Optional plugin arguments for this ingest. If omitted, the stream default is used.
        """
        if upload_mode not in ("auto", "server"):
            raise ValueError("upload_mode must be either 'auto' or 'server'")

        path = Path(file_path)
        file_size = path.stat().st_size
        init = self._init_ingestion(file_name or path.name, file_size, metadata or {}, overwrite, plugin_args)

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

        logger.debug("Pushed file %s", file_name or path.name)
        return self.get_dataset(init.dataset_id)

    def _init_ingestion(
        self,
        dataset_name: str,
        file_size: int,
        metadata: dict,
        overwrite: bool = False,
        plugin_args: str | None = None,
    ) -> IngestionInit:
        body = {
            "stream_id": self.id,
            "dataset_name": dataset_name,
            "file_size": file_size,
            "metadata": metadata,
            "overwrite": overwrite,
        }
        if plugin_args is not None:
            body["plugin_args"] = plugin_args
        r = self._client.post("/ingestion", json=body)
        return IngestionInit(**validate_response(r, "Initialize ingestion failed"))

    def _upload_server(self, init: IngestionInit, path: Path) -> None:
        with path.open("rb") as file:
            files = {"file": (path.name, file, "application/octet-stream")}
            r = self._client.post(
                f"/ingestion/{init.ingestion_id}/upload/server",
                files=files,
                timeout=self._client.STORAGE_TIMEOUT,
            )
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
        validate_storage_response(response, "Storage PUT failed")

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
                    validate_storage_response(response, f"Part {part.part_number} storage PUT failed")

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

    def update(
        self,
        *,
        name: str | None | Omitted = OMITTED,
        description: str | None | Omitted = OMITTED,
        plugin: str | None | Omitted = OMITTED,
        plugin_args: str | None | Omitted = OMITTED,
        signal_reduction: list | None | Omitted = OMITTED,
        insight_workspace: str | None | Omitted = OMITTED,
        insight_project: str | None | Omitted = OMITTED,
        scripts: list[int] | None | Omitted = OMITTED,
    ) -> "DataStream":
        """
        Update this datastream. Only provided fields are sent.

        Args:
            name: The new name for the datastream.
            description: The new description for the datastream.
            plugin: The new plugin for the datastream.
            plugin_args: The new plugin arguments for the datastream.
            signal_reduction: The new signal reduction for the datastream.
            insight_workspace: The new insight workspace for the datastream.
            insight_project: The new insight project for the datastream.
            scripts: Replace the processing pipeline with these script IDs, in
                order. Pass ``[]`` to detach all scripts. Omitted leaves the
                pipeline unchanged.
        """
        payload: dict[str, Any] = {}
        if name is not OMITTED:
            payload["name"] = name
        if description is not OMITTED:
            payload["description"] = description
        if plugin is not OMITTED:
            payload["plugin"] = plugin
        if plugin_args is not OMITTED:
            payload["plugin_args"] = plugin_args
        if signal_reduction is not OMITTED:
            payload["signal_reduction"] = signal_reduction
        if insight_workspace is not OMITTED:
            payload["insight_workspace"] = insight_workspace
        if insight_project is not OMITTED:
            payload["insight_project"] = insight_project
        if scripts is not OMITTED:
            payload["scripts"] = scripts

        r = self._client.post(f"/stream/update/{self.id}", json=payload)
        validate_response(r, "Update stream failed")
        logger.debug(f"Updated stream {self.name}: {payload}")
        return self.fetch(self._client, self.id)

    def rerun_processing(self, dataset_ids: Sequence[int] | None = None) -> None:
        """
        Rerun aliasing and processing scripts.

        Args:
            dataset_ids: The IDs of the datasets to rerun processing for. If omitted, every eligible dataset in the stream is processed.
        """
        if dataset_ids is None:
            r = self._client.post(f"/stream/{self.id}/processing")
        else:
            ids = list(dataset_ids)
            if not ids:
                raise ValueError("rerun_processing requires at least one dataset id")
            r = self._client.post(f"/stream/{self.id}/processing/datasets", json=ids)
        validate_response(r, "Rerun processing failed")
        target = f"{len(dataset_ids)} datasets" if dataset_ids is not None else "all datasets"
        logger.debug(f"Reran processing for stream {self.name} ({target})")

    def delete(self) -> None:
        """
        Delete the datastream.

        Warning:
            This is a destructive action that cannot be undone and will delete all datasets in the datastream.
        """
        r = self._client.post(f"/stream/{self.id}/delete")
        validate_response(r, "Delete stream failed")
        logger.debug(f"Deleted stream {self.name}")
