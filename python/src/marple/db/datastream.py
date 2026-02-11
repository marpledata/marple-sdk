from typing import Literal, Optional
from pathlib import Path
import json

from pydantic import BaseModel, PrivateAttr, ValidationError

from marple.db.dataset import Dataset, DatasetList
from marple.utils import DBClient, validate_response


class DataStream(BaseModel):
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
        """Get a specific dataset in this datastream by its ID or path."""

        if id is None and path is None:
            raise ValueError("Either id or path must be provided.")
        if id is not None and path is not None:
            raise ValueError("Only one of id or path can be provided.")

        r = self._client.get(f"/datapool/{self.datapool}/dataset", params={"id": id, "path": path})
        r = validate_response(r, "Get dataset failed")
        return Dataset(self._client, **r)

    def get_datasets(self) -> "DatasetList":
        """Get all datasets in this datastream."""
        r = self._client.get(f"/stream/{self.id}/datasets")
        r = validate_response(r, "Get datasets failed")
        return DatasetList.from_dicts(self._client, r.json()["datasets"])

    def push_file(
        self,
        file_path: str,
        metadata: dict | None = None,
        file_name: str | None = None,
    ) -> Dataset:
        with open(file_path, "rb") as file:
            files = {"file": file}
            data = {
                "dataset_name": file_name or Path(file_path).name,
                "metadata": json.dumps(metadata or {}),
            }

            r = self._client.post(f"/stream/{self.id}/ingest", files=files, data=data)
            return self.get_dataset(validate_response(r, "File upload failed")["dataset_id"])
