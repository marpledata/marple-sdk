"""Aliasing rules: store signals under one name, whatever they are called in the source files."""

import csv
import io
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd
from pydantic import BaseModel, Field, PrivateAttr, field_validator

from marple.db.activity import logger
from marple.utils import (
    OMITTED,
    DBClient,
    Omitted,
    StreamKey,
    resolve_stream_id,
    resolve_stream_ids,
    validate_response,
)

if TYPE_CHECKING:
    from marple.db.signal import Signal
    from marple.db.signal_stats import SignalKey, SignalStatsDetail

UPSERT_COLUMNS = ["signal_name", "signal_hierarchy", "description", "streams", "copy_metadata"]
"""Columns of the CSV file that :func:`upsert_rules` sends."""


class AliasingRule(BaseModel):
    """
    A rule that stores a signal under ``signal_name``, whatever it is called in the source file.

    After a dataset is imported, the names in ``signal_hierarchy`` are tried in order and the
    first one the dataset has is copied to a signal called ``signal_name``. The original signal
    stays; :attr:`Signal.alias_of <marple.db.Signal.alias_of>` on the copy names it. A rule is
    skipped for a dataset that already has a signal called ``signal_name`` in its file.
    ``streams`` lists the stream ids the rule applies to; an empty list means every stream.

    Rules apply when a dataset is imported. To apply new or changed rules to datasets that are
    already imported, call :meth:`~marple.db.DataStream.rerun_processing`.
    """

    id: int
    signal_name: str
    """The name the signal is stored under."""
    signal_hierarchy: list[str]
    """Original names to look for, in order of preference."""
    description: str | None = None
    streams: list[int] = Field(default_factory=list)
    """Stream ids this rule applies to. Empty means every stream."""
    copy_metadata: bool = False
    """Whether the aliased signal copies the metadata (unit, description, …) of the original."""
    created_by: str | None = None
    updated_at: float | None = None

    _client: DBClient = PrivateAttr()

    def __init__(self, client: DBClient, **kwargs):
        super().__init__(**kwargs)
        self._client = client

    @field_validator("streams", mode="before")
    @classmethod
    def _default_streams(cls, value: object) -> object:
        return value or []

    @field_validator("copy_metadata", mode="before")
    @classmethod
    def _default_copy_metadata(cls, value: object) -> object:
        # A CSV upload with an empty copy_metadata cell stores null
        return bool(value)

    def update(
        self,
        *,
        signal_name: str | Omitted = OMITTED,
        signal_hierarchy: list[str] | Omitted = OMITTED,
        description: str | None | Omitted = OMITTED,
        streams: Iterable[StreamKey] | None | Omitted = OMITTED,
        copy_metadata: bool | Omitted = OMITTED,
    ) -> "AliasingRule":
        """
        Update this rule and return the updated rule. Fields that are not given keep their value.

        Requires an admin token.

        Args:
            signal_name: The name the signal is stored under.
            signal_hierarchy: Original names to look for, in order of preference.
            description: Free text.
            streams: The streams the rule applies to (ids, names or ``DataStream`` objects).
                ``None`` or an empty list means every stream.
            copy_metadata: Whether the aliased signal copies the metadata of the original.
        """
        # The server replaces the whole rule, so send every field
        payload: dict[str, Any] = self.model_dump(include=set(UPSERT_COLUMNS))
        if not isinstance(signal_name, Omitted):
            payload["signal_name"] = signal_name
        if not isinstance(signal_hierarchy, Omitted):
            payload["signal_hierarchy"] = _hierarchy(signal_hierarchy)
        if not isinstance(description, Omitted):
            payload["description"] = description
        if not isinstance(streams, Omitted):
            payload["streams"] = resolve_stream_ids(self._client, streams)
        if not isinstance(copy_metadata, Omitted):
            payload["copy_metadata"] = copy_metadata

        r = self._client.post(f"/aliasing/{self.id}", json=payload)
        updated = AliasingRule(self._client, **validate_response(r, "Update aliasing rule failed")["rule"])
        logger.debug(f"Updated aliasing rule {updated.signal_name}")
        return updated

    def delete(self) -> None:
        """
        Delete this rule. Requires an admin token.

        Aliased signals in datasets that are already imported stay until processing is rerun.
        """
        delete_rule(self._client, self)

    def applies_to(self, stream: StreamKey) -> bool:
        """Whether this rule applies to a stream: it is in ``streams``, or ``streams`` is empty."""
        return not self.streams or resolve_stream_id(self._client, stream) in self.streams

    def get_signal_stats(self, *, stream: StreamKey | None = None) -> "SignalStatsDetail":
        """
        Statistics of the signal this rule produces (``signal_name``) over the datasets that have it.

        Args:
            stream: Only this stream. ``None`` for the whole datapool.

        Raises:
            ValueError: If no dataset has been imported with this signal yet.
        """
        from marple.db import signal_stats

        return signal_stats.get(self._client, self.signal_name, stream=stream)


def list_rules(
    client: DBClient,
    signal: "SignalKey | Signal | None" = None,
    *,
    stream: StreamKey | None = None,
) -> list[AliasingRule]:
    """See :meth:`marple.db.DB.get_aliasing_rules`."""
    r = client.get("/aliasing")
    rules = [AliasingRule(client, **rule) for rule in validate_response(r, "Get aliasing rules failed")]
    if signal is not None:
        name = _signal_name(client, signal)
        rules = [rule for rule in rules if name == rule.signal_name or name in rule.signal_hierarchy]
    stream_id = resolve_stream_id(client, stream)
    if stream_id is not None:
        rules = [rule for rule in rules if not rule.streams or stream_id in rule.streams]
    return rules


def create_rule(
    client: DBClient,
    signal_name: str,
    signal_hierarchy: list[str],
    *,
    description: str | None = None,
    streams: Iterable[StreamKey] | None = None,
    copy_metadata: bool = False,
) -> AliasingRule:
    """See :meth:`marple.db.DB.create_aliasing_rule`."""
    r = client.post(
        "/aliasing",
        json={
            "signal_name": signal_name,
            "signal_hierarchy": _hierarchy(signal_hierarchy),
            "description": description,
            "streams": resolve_stream_ids(client, streams),
            "copy_metadata": copy_metadata,
        },
    )
    rule = AliasingRule(client, **validate_response(r, "Create aliasing rule failed")["rule"])
    logger.debug(f"Created aliasing rule {signal_name}")
    return rule


def upsert_rules(
    client: DBClient, rules: "str | Path | pd.DataFrame | Iterable[dict[str, Any] | AliasingRule]"
) -> list[AliasingRule]:
    """See :meth:`marple.db.DB.upsert_aliasing_rules`."""
    rows: list[dict[Any, Any]]
    if isinstance(rules, (str, Path)):
        rows = pd.read_csv(rules, dtype=str, keep_default_na=False).to_dict(orient="records")
    elif isinstance(rules, pd.DataFrame):
        rows = rules.to_dict(orient="records")
    else:
        rows = [r.model_dump() if isinstance(r, AliasingRule) else dict(r) for r in rules]
    if not rows:
        return []

    content = _to_csv(client, rows)
    r = client.post("/aliasing-upsert", files={"file": ("aliasing_rules.csv", content, "text/csv")})
    if r.status_code in (415, 422):
        raise ValueError(f"Upsert aliasing rules failed: {r.json().get('detail', r.text)}")
    upserted = [AliasingRule(client, **rule) for rule in validate_response(r, "Upsert aliasing rules failed")]
    logger.debug(f"Upserted {len(upserted)} aliasing rule(s)")
    return upserted


def delete_rule(client: DBClient, rule: "int | AliasingRule") -> None:
    """See :meth:`marple.db.DB.delete_aliasing_rule`."""
    rule_id = rule.id if isinstance(rule, AliasingRule) else rule
    r = client.delete(f"/aliasing/{rule_id}")
    validate_response(r, "Delete aliasing rule failed")
    logger.debug(f"Deleted aliasing rule {rule_id}")


def _signal_name(client: DBClient, signal: "SignalKey | Signal") -> str:
    if isinstance(signal, str):
        return signal
    if isinstance(signal, int):
        from marple.db import signal_stats

        return signal_stats.get(client, signal).name
    return signal.name


def _hierarchy(names: "str | Iterable[str]") -> list[str]:
    if isinstance(names, str):
        raise TypeError("signal_hierarchy must be a list of original signal names, not a single string")
    return list(names)


def _to_csv(client: DBClient, rows: list[dict]) -> bytes:
    """The rules as the CSV file the server reads: lists become comma-separated cells."""
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(UPSERT_COLUMNS)
    stream_ids: dict[str, int] = {}  # resolve each stream name once, not once per row

    def stream_id(key: Any) -> int:
        if isinstance(key, str) and key.strip().isdigit():
            return int(key)
        if isinstance(key, str):
            if key not in stream_ids:
                stream_ids[key] = resolve_stream_id(client, key)  # type: ignore[assignment]
            return stream_ids[key]
        return resolve_stream_id(client, key)  # type: ignore[return-value]

    for number, row in enumerate(rows, start=1):
        name = _text(row.get("signal_name"))
        hierarchy = _cells(row.get("signal_hierarchy"))
        if not name or not hierarchy:
            raise ValueError(f"Aliasing rule {number}: signal_name and signal_hierarchy are required")
        if any("," in original for original in hierarchy):
            raise ValueError(
                f"Aliasing rule {number} ({name}): original names with a comma cannot be upserted in bulk. "
                "Use create_aliasing_rule or AliasingRule.update for this rule."
            )
        streams = [str(stream_id(s)) for s in _cells(row.get("streams"))]
        writer.writerow(
            [
                name,
                _list_cell(hierarchy),
                _text(row.get("description")),
                _list_cell(streams),
                "true" if _truthy(row.get("copy_metadata")) else "false",
            ]
        )
    return out.getvalue().encode("utf-8")


def _list_cell(values: list[str]) -> str:
    # The server parses the file with pandas, which turns a cell like `12` into a number and then fails
    # to split it. A trailing comma keeps the cell text; the server drops the empty item.
    return ",".join(values) + "," if values else ""


def _cells(value: Any) -> list[Any]:
    """A list cell given as a list or as comma-separated text."""
    if _missing(value):
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, Iterable):
        return list(value)
    return [value]


def _text(value: Any) -> str:
    return "" if _missing(value) else str(value)


def _truthy(value: Any) -> bool:
    if _missing(value):
        return False
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes", "y", "t")
    return bool(value)


def _missing(value: Any) -> bool:
    return value is None or (isinstance(value, float) and value != value)
