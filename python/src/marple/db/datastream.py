import json
from pathlib import Path
from typing import Literal, Optional

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
    ) -> Dataset:
        """
        Push a file to the datastream. The file will be ingested as a new dataset.

        Args:
            file_path: The path to the file to push.
            metadata: Optional metadata to attach to the dataset.
            file_name: Optional name for the dataset. If not provided, the file name will be used.
        """
        with open(file_path, "rb") as file:
            files = {"file": file}
            data = {
                "dataset_name": file_name or Path(file_path).name,
                "metadata": json.dumps(metadata or {}),
            }

            r = self._client.post(f"/stream/{self.id}/ingest", files=files, data=data)
            return self.get_dataset(validate_response(r, "File upload failed")["dataset_id"])

    def delete(self) -> None:
        """
        Delete the datastream.

        Warning:
            This is a destructive action that cannot be undone and will delete all datasets in the datastream.
        """
        r = self._client.post(f"/stream/{self.id}/delete")
        validate_response(r, "Delete stream failed")
