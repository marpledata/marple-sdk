"""Signals across datasets: search, statistics and import priority (the Signals page).

A signal here is a signal over many datasets, of one stream or of the whole datapool. A signal in a
single dataset is a :class:`~marple.db.Signal`; here it shows up as a :class:`SignalOccurrence`.
"""

from collections import UserList
from collections.abc import Iterable
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Union

import pandas as pd
import requests
from pydantic import BaseModel, PrivateAttr

from marple.db import aliasing
from marple.db.activity import logger
from marple.db.signal import StorageStatus
from marple.utils import (
    OMITTED,
    DBClient,
    Omitted,
    StreamKey,
    resolve_stream_id,
    validate_response,
)

if TYPE_CHECKING:
    from marple.db.aliasing import AliasingRule

SEARCH_PAGE_SIZE = 1000
"""Signals per request of :func:`search`: the most the server returns in one page."""

DATASETS_PAGE_SIZE = 1000
"""Datasets per request of :meth:`SignalStats.get_datasets`."""

PRIORITY_BATCH_SIZE = 10_000
"""Signal ids per request of :func:`set_priority`: the most the server takes at once."""


class SignalSort(StrEnum):
    """Columns :meth:`~marple.db.DB.search_signals` can sort on."""

    ID = "id"
    NAME = "name"
    IMPORTED_COUNT = "imported_count"
    COUNT = "count"
    COLD_BYTES = "cold_bytes"
    HOT_BYTES = "hot_bytes"
    ARCHIVE_BYTES = "archive_bytes"
    HEATED_COUNTER = "heated_counter"
    HEATED_LAST = "heated_last"
    IMPORTED_FIRST = "imported_first"
    IMPORTED_LAST = "imported_last"
    VALUE_MIN = "value_min"
    VALUE_MEAN = "value_mean"
    VALUE_MAX = "value_max"
    UNIT = "unit"
    FREQUENCY_MAX = "frequency_max"


class DatasetRef(BaseModel):
    """A pointer to a dataset: enough to fetch it with :meth:`~marple.db.DB.get_dataset`."""

    id: int
    stream_id: int
    path: str
    created_at: datetime
    """When the dataset was imported."""


class SignalStreamStats(BaseModel):
    """One signal in one stream."""

    stream_id: int
    imported_count: int
    coverage: float | None
    count: int
    cold_bytes: int
    hot_bytes: int
    n_hot: int
    n_archived: int
    archive_bytes: int
    heated_counter: int
    heated_last: datetime | None
    imported_first: datetime | None
    imported_last: datetime | None
    units: dict[str, int]


class SignalOccurrence(BaseModel):
    """A signal in one dataset. Its values are ``None`` for a text signal."""

    dataset: DatasetRef
    value_min: float | None
    value_max: float | None
    value_mean: float | None
    count: int | None
    """Datapoints."""
    count_text: int | None = None
    """Datapoints with text: with no numbers either, the signal is text. ``None`` when the server does not report it."""
    duration: int | None
    """Recorded duration, ns."""
    frequency: float | None
    """Sample rate, Hz."""
    unit: str | None
    original_name: str | None
    """The name the signal had in the source file, before a signal import rule renamed it."""
    storage_status: StorageStatus
    heated_counter: int


class HeatBucket(BaseModel):
    """A run of alike signals: heated equally often, of about the same size."""

    heated_counter: int
    n_signals: int
    cold_bytes: int
    hot_bytes: int
    n_occurrences: int
    n_hot_occurrences: int
    n_archived_occurrences: int


class SignalStatsSummary(BaseModel):
    """Totals over every signal, and how many signals there are of each kind."""

    n_signals: int
    n_occurrences: int
    """Signals summed over datasets: one signal in 3 datasets counts 3."""
    n_datasets: int
    count: int
    cold_bytes: int
    hot_bytes: int
    n_hot_signals: int
    n_hot_occurrences: int
    archive_bytes: int
    n_archived_occurrences: int
    n_archived: int
    n_heated: int
    n_heated_30d: int
    n_unit_conflict: int
    n_aliased: int
    """Signals imported under an original name other than their own (renamed by an import rule)."""
    n_constant: int
    n_text: int
    n_empty: int | None = None
    """
    Signals with datapoints whose values are all null: no number and no text.
    ``None`` when the server does not report it.
    """
    n_priority: int
    """Signals with high import priority in at least one stream that has them."""
    n_priority_partial: int
    n_single: int
    """Signals in exactly one dataset."""
    heat: list[HeatBucket]
    """Most heated first. Empty when the summary was asked for with a query."""
    collected_at: datetime | None = None


class SignalPriorityResult(BaseModel):
    """What :meth:`~marple.db.DB.set_signal_priority` changed."""

    signals: int
    """Signals asked for."""
    rules: int
    """Import rules changed: one per stream and original name."""


class SignalPriorityImportResult(BaseModel):
    """What :meth:`~marple.db.DB.import_signal_priority` changed."""

    signals: int
    """Signals of the file with a priority to set."""
    unknown: int
    """Of those, the signals no stream has: nothing to set."""
    skipped: int
    """Rows marked ``partial``, left as they are."""
    rules: int
    """Import rules changed: one per stream and original name."""


class SignalStats(BaseModel):
    """
    One signal, combined over the datasets that contain it.

    The statistics cover either one stream or the whole datapool, depending on how the object
    was fetched (see :attr:`stream_id`). Methods on this object stay in that scope unless a
    ``stream`` is passed explicitly.
    """

    id: int
    name: str
    n_streams: int
    imported_count: int
    """Datasets that contain the signal."""
    coverage: float | None
    """Share of the datasets that contain the signal, from 0 to 1."""
    imported_first: datetime | None
    imported_last: datetime | None
    time_min: int | None
    """Earliest timestamp, ns."""
    time_max: int | None
    """Latest timestamp, ns."""
    duration_total: int
    """Recorded duration summed over datasets, ns."""
    count: int
    """Datapoints over all datasets."""
    count_value: int
    count_text: int
    value_min: float | None
    value_max: float | None
    value_mean: float | None
    value_min_dataset: DatasetRef | None
    value_max_dataset: DatasetRef | None
    frequency_min: float | None
    frequency_max: float | None
    units: dict[str, int]
    """Datasets per unit, most common first."""
    original_names: dict[str, int]
    """
    Datasets per original name: the name in the source file, before a signal import rule renamed
    it. Signals produced by an aliasing rule are copies and list their own name here; see
    :meth:`get_aliasing_rules` and :attr:`Signal.alias_of <marple.db.Signal.alias_of>` for those.
    """
    cold_bytes: int
    cold_file_count: int
    hot_bytes: int
    n_hot: int
    n_archived: int
    archive_bytes: int
    heated_counter: int
    """Times loaded into hot storage."""
    heated_last: datetime | None
    priority_stream_ids: list[int]
    """The streams that import this signal with high priority."""

    _client: DBClient = PrivateAttr()
    _stream_id: int | None = PrivateAttr(default=None)

    def __init__(self, client: DBClient, stream_id: int | None = None, **kwargs):
        super().__init__(**kwargs)
        self._client = client
        self._stream_id = stream_id

    @property
    def stream_id(self) -> int | None:
        """The stream these statistics cover, or ``None`` when they cover the whole datapool."""
        return self._stream_id

    @property
    def priority(self) -> Literal["high", "partial", "default"]:
        """
        Import priority: ``"high"`` in every stream that has the signal, ``"partial"`` in some
        of them, ``"default"`` in none.
        """
        if not self.priority_stream_ids:
            return "default"
        return "high" if len(self.priority_stream_ids) >= self.n_streams else "partial"

    def refresh(self) -> "SignalStatsDetail":
        """Return freshly fetched statistics for this signal, in the same scope."""
        return get(self._client, self.id, stream=self._stream_id)

    def get_datasets(
        self, *, stream: StreamKey | None | Omitted = OMITTED, limit: int | None = 40
    ) -> list[SignalOccurrence]:
        """
        The datasets that contain this signal, newest first, with the signal's statistics in each.

        Args:
            stream: Only datasets of this stream. Defaults to the scope of this object;
                pass ``None`` for the whole datapool.
            limit: Maximum number of datasets. ``None`` returns all of them.
        """
        params = _stream_params(resolve_stream_id(self._client, self._scope(stream)))
        occurrences: list[SignalOccurrence] = []
        while limit is None or len(occurrences) < limit:
            remaining = DATASETS_PAGE_SIZE if limit is None else limit - len(occurrences)
            r = self._client.get(
                f"{_base(self._client)}/{self.id}/datasets",
                params={**params, "limit": min(remaining, DATASETS_PAGE_SIZE)},
            )
            page = _validate(r, f"Get datasets of signal {self.name} failed")
            occurrences.extend(SignalOccurrence.model_validate(d) for d in page["datasets"])
            if page["next"] is None:
                break
            params["before"] = page["next"]
        return occurrences

    def set_priority(
        self, high: bool = True, *, stream: StreamKey | None | Omitted = OMITTED
    ) -> SignalPriorityResult:
        """
        Give this signal high import priority, or take it away with ``high=False``.

        See :meth:`~marple.db.DB.set_signal_priority`.

        Args:
            high: ``True`` for high priority, ``False`` for the default.
            stream: Only in this stream. Defaults to the scope of this object; pass ``None``
                for every stream that has the signal.
        """
        return set_priority(self._client, self, high, stream=self._scope(stream))

    def get_aliasing_rules(self, *, stream: StreamKey | None | Omitted = OMITTED) -> "list[AliasingRule]":
        """
        The aliasing rules that produce this signal: the rules whose ``signal_name`` is its name.

        Args:
            stream: Only rules that apply to this stream. Defaults to the scope of this object.
        """
        rules = aliasing.list_rules(self._client, signal=self.name, stream=self._scope(stream))
        return [rule for rule in rules if rule.signal_name == self.name]

    def _scope(self, stream: StreamKey | None | Omitted) -> StreamKey | None:
        return self._stream_id if isinstance(stream, Omitted) else stream


class SignalStatsDetail(SignalStats):
    """All statistics of one signal, including a breakdown per stream."""

    streams: list[SignalStreamStats]
    """The signal per stream, most datasets first."""

    # Repeated for mypy: the pydantic plugin gives each model its own keyword-only __init__
    def __init__(self, client: DBClient, stream_id: int | None = None, **kwargs):
        super().__init__(client, stream_id, **kwargs)


class SignalStatsList(UserList[SignalStats]):
    """
    The result of :meth:`~marple.db.DB.search_signals`: a list of :class:`SignalStats`.

    Attributes:
        total: Number of signals that match the search, which can be more than were returned.
        collected_at: When the statistics were collected (pool-wide searches only).
    """

    def __init__(
        self,
        signals: Iterable[SignalStats] = (),
        total: int | None = None,
        collected_at: datetime | None = None,
    ):
        super().__init__(signals)
        self.total = len(self.data) if total is None else total
        self.collected_at = collected_at

    def __getitem__(self, index):  # type: ignore[override]
        # UserList builds a slice from the data alone: carry the total and collection time over
        if isinstance(index, slice):
            return SignalStatsList(self.data[index], self.total, self.collected_at)
        return self.data[index]

    def to_dataframe(self) -> pd.DataFrame:
        """One row per signal, one column per statistic, plus ``priority``."""
        return pd.DataFrame([{**s.model_dump(), "priority": s.priority} for s in self.data])


SignalKey = Union[int, str, SignalStats]
"""How a signal can be given: its id, its name, or a :class:`SignalStats`."""


def search(
    client: DBClient,
    query: str | None = None,
    *,
    stream: StreamKey | None = None,
    sort: SignalSort | str = SignalSort.IMPORTED_COUNT,
    descending: bool = True,
    limit: int | None = 50,
) -> SignalStatsList:
    """See :meth:`marple.db.DB.search_signals`."""
    stream_id = resolve_stream_id(client, stream)
    params = _search_params(stream_id, query, sort, descending)
    result = SignalStatsList()
    while limit is None or len(result) < limit:
        remaining = SEARCH_PAGE_SIZE if limit is None else limit - len(result)
        r = client.get(
            _base(client),
            params={**params, "limit": min(remaining, SEARCH_PAGE_SIZE), "offset": len(result)},
        )
        page = _validate(r, "Search signals failed")
        result.extend(SignalStats(client, stream_id, **s) for s in page["signals"])
        result.total = page["total"]
        result.collected_at = _parse_datetime(page.get("collected_at"))
        if not page["signals"] or len(result) >= result.total:
            break
    return result


def get(client: DBClient, signal: SignalKey, *, stream: StreamKey | None = None) -> SignalStatsDetail:
    """See :meth:`marple.db.DB.get_signal_stats`."""
    stream_id = resolve_stream_id(client, stream)
    signal_id = resolve_signal_id(client, signal)
    r = client.get(f"{_base(client)}/{signal_id}", params=_stream_params(stream_id))
    if r.status_code == 404:
        scope = "the datapool" if stream_id is None else f"stream {stream_id}"
        raise ValueError(f"No signal stats for signal {_label(signal)} in {scope}")
    return SignalStatsDetail(client, stream_id, **_validate(r, f"Get signal stats of {_label(signal)} failed"))


def get_summary(
    client: DBClient, query: str | None = None, *, stream: StreamKey | None = None
) -> SignalStatsSummary:
    """See :meth:`marple.db.DB.get_signal_stats_summary`."""
    params = _stream_params(resolve_stream_id(client, stream))
    if query:
        params["q"] = query
    r = client.get(f"{_base(client)}/summary", params=params)
    return SignalStatsSummary.model_validate(_validate(r, "Get signal stats summary failed"))


def export(
    client: DBClient,
    path: str | Path,
    query: str | None = None,
    *,
    stream: StreamKey | None = None,
    sort: SignalSort | str = SignalSort.IMPORTED_COUNT,
    descending: bool = True,
) -> Path:
    """See :meth:`marple.db.DB.export_signal_stats`."""
    destination = Path(path)
    if destination.is_dir():
        destination = destination / "signals.csv"
    params = _search_params(resolve_stream_id(client, stream), query, sort, descending)
    with client.get(f"{_base(client)}/export", params=params, stream=True) as r:
        if r.status_code != 200:
            _validate(r, "Export signal stats failed")
        with open(destination, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 16):
                f.write(chunk)
    return destination


def set_priority(
    client: DBClient,
    signals: SignalKey | Iterable[SignalKey],
    high: bool = True,
    *,
    stream: StreamKey | None = None,
) -> SignalPriorityResult:
    """See :meth:`marple.db.DB.set_signal_priority`."""
    params = _stream_params(resolve_stream_id(client, stream))
    keys = [signals] if isinstance(signals, (int, str, SignalStats)) else list(signals)
    signal_ids = [resolve_signal_id(client, key) for key in keys]
    priority = "high" if high else "default"
    total = SignalPriorityResult(signals=0, rules=0)
    for start in range(0, len(signal_ids), PRIORITY_BATCH_SIZE):
        r = client.post(
            f"{_base(client)}/priority",
            params=params,
            json={"signal_ids": signal_ids[start : start + PRIORITY_BATCH_SIZE], "priority": priority},
        )
        batch = SignalPriorityResult.model_validate(_validate(r, "Set signal priority failed"))
        total.signals += batch.signals
        total.rules += batch.rules
    logger.debug(f"Set priority {priority} on {total.signals} signal(s): {total.rules} import rule(s) changed")
    return total


def import_priority(
    client: DBClient, csv_path: str | Path, *, stream: StreamKey | None = None
) -> SignalPriorityImportResult:
    """See :meth:`marple.db.DB.import_signal_priority`."""
    csv_path = Path(csv_path)
    params = _stream_params(resolve_stream_id(client, stream))
    with open(csv_path, "rb") as f:
        r = client.post(
            f"{_base(client)}/priority/import", params=params, files={"file": (csv_path.name, f, "text/csv")}
        )
    result = SignalPriorityImportResult.model_validate(_validate(r, "Import signal priority failed"))
    logger.debug(f"Imported signal priority from {csv_path.name}: {result.rules} import rule(s) changed")
    return result


def resolve_signal_id(client: DBClient, signal: SignalKey) -> int:
    """The id of a signal given as an id, a name or a :class:`SignalStats`."""
    if isinstance(signal, SignalStats):
        return signal.id
    if isinstance(signal, str):
        signal_id = client.get_signal_id(signal)
        if signal_id is None:
            raise ValueError(f"Signal {signal!r} not found in datapool {client.datapool}")
        return signal_id
    return signal


def _base(client: DBClient) -> str:
    return f"/datapool/{client.datapool}/signal-stats"


def _stream_params(stream_id: int | None) -> dict[str, Any]:
    return {} if stream_id is None else {"stream_id": stream_id}


def _search_params(
    stream_id: int | None, query: str | None, sort: SignalSort | str, descending: bool
) -> dict[str, Any]:
    try:
        sort = SignalSort(sort)
    except ValueError:
        raise ValueError(f"Cannot sort signals on {sort!r}. Choose from: {', '.join(SignalSort)}") from None
    params = {"sort": sort.value, "descending": descending, **_stream_params(stream_id)}
    if query:
        params["q"] = query
    return params


def _validate(response: requests.Response, failure_message: str) -> Any:
    """Like `validate_response` for the bare models of the stats endpoints, with their 503 explained."""
    if response.status_code == 503:
        raise RuntimeError(
            f"{failure_message}: the signal stats of this datapool are still being collected. Retry in a moment."
        )
    return validate_response(response, failure_message, check_envelope=False)


def _parse_datetime(value: str | None) -> datetime | None:
    return None if value is None else datetime.fromisoformat(value)


def _label(signal: SignalKey) -> str:
    return signal.name if isinstance(signal, SignalStats) else str(signal)
