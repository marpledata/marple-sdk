from typing import Literal, Optional

from pydantic import BaseModel, PrivateAttr, ValidationError

from marple.db.dataset import Dataset, DatasetList
from marple.utils import DBSession


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

    _known_datasets: dict[str, int] = PrivateAttr(default_factory=dict)
    _datasets: dict[int, "Dataset"] = PrivateAttr(default_factory=dict)
    _session = PrivateAttr()

    def __init__(self, session: DBSession, **kwargs):
        super().__init__(**kwargs)
        self._session = session

    def get_dataset(self, id: int | None = None, path: str | None = None) -> "Dataset":
        """Get a specific dataset in this datastream by its ID or path."""

        if id is None and path is None:
            raise ValueError("Either id or path must be provided.")
        if id is not None and path is not None:
            raise ValueError("Only one of id or path can be provided.")

        id = id if path is None else self.get_dataset_id(path)

        if id is None:
            raise ValueError(f"Dataset with path {path} not found in datastream {self.name}.")

        if id not in self._datasets:
            r = self._session.get(f"/stream/{self.id}/dataset/{id}")
            dataset = Dataset(session=self._session, **r.json())
            self._datasets[id] = dataset
            self._known_datasets[dataset.path] = dataset.id
        return self._datasets[id]

    def get_dataset_id(self, dataset_path: str) -> int | None:
        if dataset_path not in self._known_datasets:
            self.get_datasets()
        return self._known_datasets.get(dataset_path)

    def get_datasets(self) -> "DatasetList":
        """Get all datasets in this datastream."""
        if self.n_datasets is None or len(self._datasets) < self.n_datasets:
            r = self._session.get(f"/stream/{self.id}/datasets")
            for dataset in r.json():
                try:
                    dataset_obj = Dataset(session=self._session, **dataset)
                except ValidationError as e:
                    raise UserWarning(
                        f"Failed to parse dataset with id {dataset.get('id')} and path {dataset.get('path')}. Skipping. Error: {e}"
                    )
                self._datasets[dataset_obj.id] = dataset_obj
                self._known_datasets[dataset_obj.path] = dataset_obj.id
            self._has_all_datasets = True
        return DatasetList(self._datasets.values())
