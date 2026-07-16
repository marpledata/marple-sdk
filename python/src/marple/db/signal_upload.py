"""Helpers for lake-native signal upload (presign → write → PUT → complete)."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from marple.db.dataset import Dataset

import tempfile
from concurrent.futures import ThreadPoolExecutor
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, Sequence

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pydantic import BaseModel, ConfigDict, Field

from marple.db.constants import (
    COL_DATASET,
    COL_SIG,
    COL_TIME,
    COL_VAL,
    COL_VAL_TEXT,
    LAKE_ARROW_SCHEMA,
    LAKE_PARQUET_SCHEMA,
    MAX_ROWS_PER_FILE,
    ROW_GROUP_SIZE,
)
from marple.utils import DBClient, validate_response, validate_storage_response


class SignalsAlreadyExistError(ValueError):
    """Raised when presign fails with HTTP 409 (duplicate / exists / size invalid)."""

    def __init__(self, signals: list[dict[str, Any]], message: str | None = None):
        self.signals = signals
        detail = message or "One or more signals failed upload validation"
        checks = ", ".join(f"{s.get('name')}: {s.get('status')}" for s in signals)
        super().__init__(f"{detail}: {checks}" if checks else detail)


class SignalUpload(BaseModel):
    """Input for :meth:`Dataset.add_signal` / :meth:`Dataset.add_signals`.

    ``data`` is a DataFrame, Arrow table, or parquet path with ``time`` and
    ``value`` and/or ``value_text``.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    data: pd.DataFrame | pa.Table | Path | str
    metadata: dict[str, Any] = Field(default_factory=dict)
    overwrite: bool = False
    priority: Literal["default", "high"] = "default"

    def plan_upload(self, dataset: "Dataset") -> "PlannedUpload":
        data = self._validate_data(dataset)
        row_counts = _plan_row_counts(data.num_rows)
        frequency = _estimate_frequency(data.column(COL_TIME))
        return PlannedUpload(
            name=self.name,
            data=data,
            metadata=self.metadata,
            overwrite=self.overwrite,
            priority=SignalImportPriority(self.priority),
            row_counts=row_counts,
            frequency=frequency,
        )

    def _validate_data(self, dataset: "Dataset") -> pa.Table:
        if isinstance(self.data, pa.Table):
            table = self.data
        elif isinstance(self.data, pd.DataFrame):
            table = pa.Table.from_pandas(self.data, preserve_index=False)
        elif isinstance(self.data, (Path, str)):
            table = pq.read_table(self.data)
        else:
            raise ValueError(f"{self.name}: Invalid data type: {type(self.data)}")

        if table.num_rows == 0:
            raise ValueError(f"{self.name}: Signal must have at least one row")

        names = set(table.column_names)
        if COL_TIME not in names:
            raise ValueError(f"{self.name}: Data must include a {COL_TIME!r} column")
        if COL_VAL not in names and COL_VAL_TEXT not in names:
            raise ValueError(f"{self.name}: Data must include {COL_VAL!r} and/or {COL_VAL_TEXT!r}")
        # Cast to lake signal schema (time / value / value_text); fail loud on unsafe casts.
        n = table.num_rows

        time = self._validate_time_range(table, dataset)

        try:
            if COL_VAL in names:
                value = table.column(COL_VAL).cast(pa.float64(), safe=True)
                value = pc.if_else(pc.is_finite(value), value, pa.nulls(n, type=pa.float64()))
            else:
                value = pa.nulls(n, type=pa.float64())
        except (pa.ArrowInvalid, pa.ArrowTypeError) as exc:
            raise ValueError(f"{self.name}: {COL_VAL!r} must be float64-compatible: {exc}") from exc

        try:
            if COL_VAL_TEXT in names:
                value_text = table.column(COL_VAL_TEXT).cast(pa.string(), safe=True)
            else:
                value_text = pa.nulls(n, type=pa.string())
        except (pa.ArrowInvalid, pa.ArrowTypeError) as exc:
            raise ValueError(f"{self.name}: {COL_VAL_TEXT!r} must be string-compatible: {exc}") from exc

        return pa.Table.from_arrays([time, value, value_text], schema=LAKE_ARROW_SCHEMA)

    def _validate_time_range(self, table: pa.Table, dataset: "Dataset") -> pa.ChunkedArray | pa.Array:
        try:
            time = table.column(COL_TIME).cast(pa.int64(), safe=True)
        except (pa.ArrowInvalid, pa.ArrowTypeError) as exc:
            raise ValueError(f"{self.name}: {COL_TIME!r} must be int64-compatible: {exc}") from exc

        if time.null_count:
            raise ValueError(f"{self.name}: {COL_TIME!r} must not contain nulls")

        time_min = pc.min(time).as_py()
        if time_min < 0:
            raise ValueError(f"{self.name}: {COL_TIME!r} must be greater than or equal to 0")

        if dataset.timestamp_start is not None and dataset.timestamp_stop is not None:
            time_max = pc.max(time).as_py()
            if time_max < dataset.timestamp_start or time_min > dataset.timestamp_stop:
                raise ValueError(
                    f"{self.name}: Signal time range [{time_min}, {time_max}] does not overlap "
                    f"dataset range [{dataset.timestamp_start}, {dataset.timestamp_stop}]"
                )

        return time


def _plan_row_counts(num_rows: int) -> list[int]:
    full, remainder = divmod(num_rows, MAX_ROWS_PER_FILE)
    return [MAX_ROWS_PER_FILE] * full + ([remainder] if remainder else [])


def _estimate_frequency(time_col: pa.ChunkedArray | pa.Array) -> float | None:
    chunks = [time_col] if isinstance(time_col, pa.Array) else time_col.chunks
    median_diffs: list[float] = []
    for chunk in chunks:
        if len(chunk) < 2:
            continue
        diff = pc.pairwise_diff(chunk)
        median_diff = pc.approximate_median(diff).as_py()
        if median_diff is not None and median_diff > 0:
            median_diffs.append(float(median_diff))
    return 1e9 / min(median_diffs) if median_diffs else None


class SignalImportPriority(StrEnum):
    DEFAULT = "default"
    HIGH = "high"


class PlannedUpload(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    data: pa.Table
    metadata: dict[str, Any]
    overwrite: bool
    priority: SignalImportPriority
    row_counts: list[int]
    frequency: float | None

    def to_request(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "metadata": self.metadata,
            "files": [{"index": i, "rows": rows} for i, rows in enumerate(self.row_counts)],
            "overwrite": self.overwrite,
            "priority": self.priority,
        }


class PresignedParquetFile(BaseModel):
    index: int
    rows: int
    path: str
    url: str
    expires_in: int


class PresignedSignal(BaseModel):
    name: str
    signal_id: int
    files: list[PresignedParquetFile]


class ParquetUploadMetadata(BaseModel):
    path: str
    size: int
    footer: int
    """Footer size in bytes"""


class SignalUploadStats(BaseModel):
    sum: float | None
    frequency: float | None


class SignalUploadComplete(BaseModel):
    id: int
    priority: SignalImportPriority
    stats: SignalUploadStats
    files: list[ParquetUploadMetadata]


def run_signal_uploads(
    dataset: "Dataset",
    signals: Sequence[SignalUpload | dict[str, Any]],
    *,
    concurrency: int = 4,
) -> list[int]:
    """Presign, write lake parquet, PUT, and complete. Returns allocated signal IDs."""
    planned = [
        (s if isinstance(s, SignalUpload) else SignalUpload.model_validate(s)).plan_upload(dataset)
        for s in signals
    ]
    presigned_signals = _presign_signals(dataset, planned)

    def _upload_one(plan: PlannedUpload) -> SignalUploadComplete:
        return _run_signal_upload(dataset, plan, presigned_signals[plan.name])

    workers = max(concurrency, 1)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        completed = list(executor.map(_upload_one, planned))

    complete = dataset._client.post(
        f"/stream/{dataset.datastream_id}/dataset/{dataset.id}/signal/uploads/complete",
        json={"signals": [c.model_dump() for c in completed]},
    )
    validate_response(complete, "Complete signal uploads failed")

    return [c.id for c in completed]


def _presign_signals(dataset: "Dataset", planned: list[PlannedUpload]) -> dict[str, PresignedSignal]:
    body = {"signals": [p.to_request() for p in planned]}
    r = dataset._client.post(f"/stream/{dataset.datastream_id}/dataset/{dataset.id}/signal/uploads", json=body)
    if r.status_code == 409:
        try:
            body = r.json()
        except ValueError:
            body = {}
        raise SignalsAlreadyExistError(
            body.get("signals") or [],
            message=f"Presign signal uploads failed: {body.get('error', 'signals_already_exist')}",
        )
    signals = {
        item["name"]: PresignedSignal.model_validate(item)
        for item in validate_response(r, "Presign signal uploads failed")
    }
    missing = {p.name for p in planned} - signals.keys()
    if missing:
        raise ValueError(f"Presign missing signals: {sorted(missing)}")
    if dataset._client._signal_map is not None:
        for name, signal in signals.items():
            dataset._client._signal_map[name] = signal.signal_id
    return signals


def _run_signal_upload(
    dataset: "Dataset",
    planned: PlannedUpload,
    signal: PresignedSignal,
) -> SignalUploadComplete:
    files = {file.index: file for file in signal.files}
    metadata, sums, offset = [], [], 0
    with tempfile.TemporaryDirectory() as tmp:
        signal_dir = Path(tmp)
        for index, rows in enumerate(planned.row_counts):
            local_path = signal_dir / f"part_{index}.parquet"
            part = planned.data.slice(offset, rows)  # still 3 cols
            lake = pa.table(
                {
                    COL_DATASET: pa.repeat(pa.scalar(dataset.id, type=pa.int64()), rows),
                    COL_SIG: pa.repeat(pa.scalar(signal.signal_id, type=pa.int64()), rows),
                    COL_TIME: part.column(COL_TIME),
                    COL_VAL: part.column(COL_VAL),
                    COL_VAL_TEXT: part.column(COL_VAL_TEXT),
                },
                schema=LAKE_PARQUET_SCHEMA,
            )
            pq.write_table(lake, local_path, compression="zstd", row_group_size=ROW_GROUP_SIZE)
            metadata.append(_upload_parquet(dataset._client, files[index], local_path))
            sums.append(pc.sum(lake.column(COL_VAL)).as_py())
            offset += rows

    sums = [s for s in sums if s is not None]
    return SignalUploadComplete(
        id=signal.signal_id,
        priority=planned.priority,
        stats=SignalUploadStats(sum=sum(sums) if sums else None, frequency=planned.frequency),
        files=metadata,
    )


def _upload_parquet(client: DBClient, remote: PresignedParquetFile, local_path: Path) -> ParquetUploadMetadata:
    with local_path.open("rb") as file:
        response = client.storage_session.put(remote.url, data=file)
        file.seek(-8, 2)
        footer = int.from_bytes(file.read(4), "little")
    validate_storage_response(response, "Storage PUT failed")
    return ParquetUploadMetadata(path=remote.path, size=local_path.stat().st_size, footer=footer)
