import re
import warnings
from collections import UserList
from typing import Callable, Iterable, Literal, Optional, Sequence

import pandas as pd
from pandas._typing import AggFuncType, Frequency
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, ValidationError

from marple.db.constants import COL_TIME, COL_VAL
from marple.db.signal import Signal
from marple.utils import DBSession, validate_response


class Dataset(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    id: int
    datastream_id: int = Field(alias="stream_id")
    datastream_version: int | None
    created_at: float
    created_by: str | None
    import_status: str
    import_progress: float | None
    import_message: str | None
    import_time: float | None
    path: str
    metadata: dict
    cold_path: str
    cold_bytes: int
    hot_bytes: int
    backup_path: str | None
    backup_size: int | None
    plugin: str | None
    plugin_args: str | None
    n_datapoints: int | None
    n_signals: int | None
    timestamp_start: int | None
    timestamp_stop: int | None
    import_speed: float | None
    parquet_version: int

    _session: DBSession = PrivateAttr()
    _known_signals: dict[str, int] = PrivateAttr(default_factory=dict)
    _signals: dict[int, "Signal"] = PrivateAttr(default_factory=dict)

    def __init__(self, session: DBSession, **kwargs):
        super().__init__(**kwargs)
        self._session = session

    def get_signal(self, name: str | None = None, id: int | None = None) -> Optional["Signal"]:
        """Get a specific signal in this dataset by its name or ID."""
        if name is None and id is None:
            raise ValueError("Either name or id must be provided.")
        if name is not None and id is not None:
            raise ValueError("Only one of name or id can be provided.")

        if name is not None:
            if name not in self._known_signals:
                r = self._session.get(f"/datapool/{self._session.datapool}/signal/{name}/id")
                result = validate_response(r, f"Get signal ID for signal name {name} failed")
                self._known_signals[name] = result["id"]
            id = self._known_signals.get(name)

        if id is None:
            raise ValueError(f"Signal with name {name} not found in dataset with id {self.id}.")

        if id not in self._signals:
            r = self._session.get(f"/stream/{self.datastream_id}/dataset/{self.id}/signal/{id}")
            try:
                result = validate_response(r, f"Get signal data for signal ID {id} failed")
            except Exception:
                warnings.warn(f"Failed to get signal with id {id} and name {name}.")
                return

            signal = Signal(
                session=self._session, datastream_id=self.datastream_id, dataset_id=self.id, **r.json()
            )
            self._signals[signal.id] = signal
            self._known_signals[signal.name] = signal.id

        return self._signals[id]

    def get_signals(self, signal_names: Sequence[str | re.Pattern] | None = None) -> list["Signal"]:

        compiled_filters = [
            re.compile(f"^{re.escape(f)}$") if isinstance(f, str) else f for f in signal_names or []
        ]

        def include(signal_name: str) -> bool:
            if signal_names is None:
                return True
            return any(pattern.match(signal_name) for pattern in compiled_filters)

        if len(self._signals) < self.n_signals:
            r = self._session.get(f"/stream/{self.datastream_id}/dataset/{self.id}/signals")
            for signal in r.json():
                try:
                    signal_obj = Signal(
                        session=self._session, datastream_id=self.datastream_id, dataset_id=self.id, **signal
                    )
                except ValidationError as e:
                    raise UserWarning(
                        f"Failed to parse signal with id {signal.get('id')} and name {signal.get('name')}. Skipping. Error: {e}"
                    )
                self._signals[signal_obj.id] = signal_obj
                self._known_signals[signal_obj.name] = signal_obj.id

        return [signal for signal in self._signals.values() if include(signal.name)]

    def get_data(
        self,
        signals: Sequence[str | re.Pattern],
        resample_rule: Optional[Frequency] = None,
        resample_aggregate: AggFuncType = "mean",
        **kwargs,
    ) -> pd.DataFrame:
        """
        Get the data for this dataset for the specified signals as a pandas DataFrame.

        Each dataframe contains a time column and one column for each signal.
        The dataframe is resampled according to the `resample_rule` parameter, which is passed to pandas `resample` function.
        If `resample_rule` is None, the original data is returned.
        The `resample_aggregate` parameter determines how to aggregate if there are multiple values for the same time period during resampling.
        Extra keyword arguments are passed to the pandas `resample` function.
        """
        signal_objs = self.get_signals(signal_names=signals)
        df = pd.DataFrame(columns=[COL_TIME])
        for signal_obj in signal_objs:
            signal_df = signal_obj.get_data().rename(columns={COL_VAL: signal_obj.name})
            df = df.merge(signal_df, on=COL_TIME, how="outer")
        df = df.set_index(COL_TIME)
        if resample_rule is not None:
            df = df.resample(resample_rule, **kwargs).agg(resample_aggregate)  # type: ignore
        return df


class DatasetList(UserList[Dataset]):

    def __init__(self, datasets: Iterable[Dataset]):
        super().__init__(datasets)

    def where_metadata(
        self, metadata: dict[str, int | str | Iterable[int | str]] | None = None
    ) -> "DatasetList":
        """
        Filter datasets by their metadata fields.

        Each key in the `metadata` dictionary corresponds to a metadata field name,
        and the associated value is either a single value or an iterable of values.
        A dataset is included in the results if its metadata field matches any of the specified values for all fields.
        """
        results = DatasetList([])
        cleaned_metadata = {k: [v] if not isinstance(v, list) else v for k, v in (metadata or {}).items()}
        for dataset in self.data:
            if any(dataset.metadata.get(field) not in values for field, values in cleaned_metadata.items()):
                continue
            results.append(dataset)
        return results

    def where_dataset(
        self,
        stat: Literal[
            "created_at",
            "created_by",
            "import_status",
            "import_progress",
            "import_time",
            "cold_bytes",
            "hot_bytes",
            "n_datapoints",
            "n_signals",
            "timestamp_start",
            "timestamp_stop",
        ],
        greater_than: float | None = None,
        less_than: float | None = None,
        equals: float | str | None = None,
    ) -> "DatasetList":
        """
        Filter datasets by their statistics.

        If multiple conditions are provided, a dataset must satisfy all of them to be included in the results.
        """
        results = DatasetList([])
        for dataset in self.data:
            value = getattr(dataset, stat)
            if greater_than is not None and value <= greater_than:
                continue
            if less_than is not None and value >= less_than:
                continue
            if equals is not None and value != equals:
                continue
            results.append(dataset)
        return results

    def where_signal(
        self,
        signal_name: str,
        stat: Literal[
            "cold_bytes",
            "hot_bytes",
            "count",
            "count_value",
            "count_text",
            "time_min",
            "time_max",
            "max",
            "min",
            "sum",
            "mean",
            "frequency",
        ],
        greater_than: float | None = None,
        less_than: float | None = None,
        equals: float | str | None = None,
    ) -> "DatasetList":
        """
        Filter datasets by the statistics of a specific signal.
        """
        results = DatasetList([])
        for dataset in self.data:
            signal = dataset.get_signal(signal_name)
            if signal is None:
                continue
            if stat in ["max", "min", "sum", "mean", "frequency"]:
                value = signal.stats.get(stat)
            else:
                value = getattr(signal, stat)
            if value is None:
                raise ValueError(f"Stat {stat} not found in {dataset}. ")
            if greater_than is not None and not value > greater_than:
                continue
            if less_than is not None and not value < less_than:
                continue
            if equals is not None and not value == equals:
                continue
            results.append(dataset)
        return results

    def where_predicate(self, predicate: Callable[[Dataset], bool]) -> "DatasetList":
        return DatasetList([d for d in self.data if predicate(d)])

    def get_data(
        self,
        signals: Sequence[str | re.Pattern],
        resample_rule: None | Frequency = None,
        resample_aggregate: AggFuncType = "mean",
        **kwargs,
    ) -> Iterable[tuple[Dataset, pd.DataFrame]]:
        """
        Get the data for all datasets in this list for the specified signals. Returns an iterable of (dataset, dataframe) tuples.

        Each dataframe contains the data for one dataset with a time column and one column for each signal.
        The dataframe is resampled according to the `resampling` parameter, which is passed to pandas `resample` function.
        If `resampling` is None, the original data is returned.
        """
        for dataset in self.data:
            yield dataset, dataset.get_data(
                signals=signals,
                resample_rule=resample_rule,
                resample_aggregate=resample_aggregate,
                **kwargs,
            )
