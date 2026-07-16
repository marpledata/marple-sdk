"""Helpers for lake-native signal upload (presign → write → PUT → complete)."""

from __future__ import annotations

import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Literal, Self, Sequence

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from pydantic import BaseModel, ConfigDict, Field, model_validator

from marple.db.constants import (
    COL_DATASET,
    COL_SIG,
    COL_TIME,
    COL_VAL,
    COL_VAL_TEXT,
    LAKE_PARQUET_SCHEMA,
    MAX_ROWS_PER_FILE,
    MAX_SIGNALS_PER_ADD,
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
    """One signal to add via :meth:`~marple.db.dataset.Dataset.add_signals`."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    data: pd.DataFrame | None = None
    path: Path | str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    overwrite: bool = False
    priority: Literal["default", "high"] = "default"

    @model_validator(mode="after")
    def _exactly_one_source(self) -> Self:
        if (self.data is None) == (self.path is None):
            raise ValueError("Provide exactly one of data or path")
        return self


@dataclass(frozen=True)
class WrittenParquet:
    local_path: Path
    size: int
    footer: int
    rows: int


@dataclass(frozen=True)
class PlannedUpload:
    upload: SignalUpload
    row_counts: list[int]
    frequency: float


def parquet_footer_size(path: Path | str) -> int:
    """Return the parquet footer length in bytes (excluding the 8-byte trailer)."""
    with open(path, "rb") as f:
        f.seek(-8, 2)
        return int.from_bytes(f.read(4), "little")


def plan_file_rows(total_rows: int, max_rows_per_file: int = MAX_ROWS_PER_FILE) -> list[int]:
    """Split a row count into per-file sizes (full files first, at most one remainder)."""
    if total_rows <= 0:
        raise ValueError("Signal must have at least one row")
    if max_rows_per_file <= 0:
        raise ValueError("max_rows_per_file must be positive")

    rows: list[int] = []
    remaining = total_rows
    while remaining > max_rows_per_file:
        rows.append(max_rows_per_file)
        remaining -= max_rows_per_file
    rows.append(remaining)
    return rows


def estimate_frequency(times: np.ndarray) -> float:
    """Estimate sample frequency in Hz from nanosecond timestamps; default 1.0."""
    if len(times) < 2:
        return 1.0
    diffs = np.diff(times.astype(np.int64))
    positive = diffs[diffs > 0]
    if len(positive) == 0:
        return 1.0
    median_diff = float(np.median(positive))
    if median_diff <= 0:
        return 1.0
    return 1e9 / median_diff


def _require_signal_columns(names: set[str], *, kind: str) -> None:
    if COL_TIME not in names:
        raise ValueError(f"{kind} must include a {COL_TIME!r} column")
    if COL_VAL not in names and COL_VAL_TEXT not in names:
        raise ValueError(f"{kind} must include {COL_VAL!r} and/or {COL_VAL_TEXT!r}")


class LakeParquetWriter:
    """Buffers batches and writes zstd lake parquet with ~1M-row groups / ~16M-row files."""

    def __init__(
        self,
        dest_dir: Path,
        dataset_id: int,
        signal_id: int,
        *,
        row_group_size: int = ROW_GROUP_SIZE,
        max_rows_per_file: int = MAX_ROWS_PER_FILE,
        prefix: str = "part",
    ):
        self._dest_dir = dest_dir
        self._dataset_id = dataset_id
        self._signal_id = signal_id
        self._row_group_size = row_group_size
        self._max_rows_per_file = max_rows_per_file
        self._prefix = prefix
        self._buffer: list[pa.Table] = []
        self._buffer_rows = 0
        self._file_rows = 0
        self._file_index = 0
        self._writer: pq.ParquetWriter | None = None
        self._current_path: Path | None = None
        self.files: list[WrittenParquet] = []

    def write_table(self, table: pa.Table) -> None:
        if table.num_rows == 0:
            return
        lake = self._to_lake_table(table)
        offset = 0
        while offset < lake.num_rows:
            if self._file_rows >= self._max_rows_per_file:
                self._roll_file()
            if self._buffer_rows >= self._row_group_size:
                self._flush_buffer()

            space_in_file = self._max_rows_per_file - self._file_rows
            space_in_group = self._row_group_size - self._buffer_rows
            take = min(space_in_file, space_in_group, lake.num_rows - offset)
            chunk = lake.slice(offset, take)
            self._buffer.append(chunk)
            self._buffer_rows += chunk.num_rows
            self._file_rows += chunk.num_rows
            offset += chunk.num_rows

            if self._buffer_rows >= self._row_group_size:
                self._flush_buffer()
            if self._file_rows >= self._max_rows_per_file:
                self._roll_file()

    def close(self) -> list[WrittenParquet]:
        self._flush_buffer()
        self._close_writer()
        if not self.files:
            raise ValueError("No rows written to lake parquet")
        return self.files

    def _to_lake_table(self, table: pa.Table) -> pa.Table:
        n = table.num_rows
        names = set(table.column_names)
        _require_signal_columns(names, kind="Parquet/table")

        time = table.column(COL_TIME).combine_chunks().cast(pa.int64())
        value = (
            table.column(COL_VAL).combine_chunks().cast(pa.float64())
            if COL_VAL in names
            else pa.nulls(n, type=pa.float64())
        )
        value_text = (
            table.column(COL_VAL_TEXT).combine_chunks().cast(pa.string())
            if COL_VAL_TEXT in names
            else pa.nulls(n, type=pa.string())
        )
        return pa.table(
            {
                COL_DATASET: pa.repeat(pa.scalar(self._dataset_id, type=pa.int64()), n),
                COL_SIG: pa.repeat(pa.scalar(self._signal_id, type=pa.int64()), n),
                COL_TIME: time,
                COL_VAL: value,
                COL_VAL_TEXT: value_text,
            },
            schema=LAKE_PARQUET_SCHEMA,
        )

    def _ensure_writer(self) -> pq.ParquetWriter:
        if self._writer is None:
            self._current_path = self._dest_dir / f"{self._prefix}_{self._file_index:04d}.parquet"
            self._writer = pq.ParquetWriter(self._current_path, LAKE_PARQUET_SCHEMA, compression="zstd")
        return self._writer

    def _flush_buffer(self) -> None:
        if not self._buffer:
            return
        writer = self._ensure_writer()
        table = pa.concat_tables(self._buffer)
        writer.write_table(table, row_group_size=table.num_rows)
        self._buffer.clear()
        self._buffer_rows = 0

    def _close_writer(self) -> None:
        if self._writer is None or self._current_path is None:
            return
        rows = self._file_rows
        self._writer.close()
        path = self._current_path
        self.files.append(
            WrittenParquet(
                local_path=path,
                size=path.stat().st_size,
                footer=parquet_footer_size(path),
                rows=rows,
            )
        )
        self._writer = None
        self._current_path = None
        self._file_index += 1
        self._file_rows = 0

    def _roll_file(self) -> None:
        self._flush_buffer()
        self._close_writer()


def _inspect_source(upload: SignalUpload, frequency_sample_limit: int = 10_000) -> tuple[int, np.ndarray]:
    """Return (total_rows, time sample) with a single source open."""
    if upload.data is not None:
        times = upload.data[COL_TIME].to_numpy()
        return len(upload.data), times[:frequency_sample_limit]

    assert upload.path is not None
    pf = pq.ParquetFile(upload.path)
    total_rows = pf.metadata.num_rows
    batches = []
    rows = 0
    for batch in pf.iter_batches(batch_size=min(frequency_sample_limit, ROW_GROUP_SIZE), columns=[COL_TIME]):
        batches.append(batch.column(0))
        rows += batch.num_rows
        if rows >= frequency_sample_limit:
            break
    if not batches:
        return total_rows, np.array([], dtype=np.int64)
    sample = pa.chunked_array(batches).combine_chunks().slice(0, frequency_sample_limit).to_numpy()
    return total_rows, sample


def _iter_source_tables(upload: SignalUpload, batch_size: int = ROW_GROUP_SIZE) -> Iterator[pa.Table]:
    if upload.data is not None:
        df = upload.data
        _require_signal_columns(set(df.columns), kind="DataFrame")
        for start in range(0, len(df), batch_size):
            chunk = df.iloc[start : start + batch_size]
            arrays: dict[str, Any] = {COL_TIME: pa.array(chunk[COL_TIME], type=pa.int64())}
            if COL_VAL in chunk.columns:
                arrays[COL_VAL] = pa.array(pd.to_numeric(chunk[COL_VAL], errors="coerce"), type=pa.float64())
            if COL_VAL_TEXT in chunk.columns:
                texts = [None if pd.isna(v) else str(v) for v in chunk[COL_VAL_TEXT]]
                arrays[COL_VAL_TEXT] = pa.array(texts, type=pa.string())
            yield pa.table(arrays)
        return

    assert upload.path is not None
    pf = pq.ParquetFile(upload.path)
    names = set(pf.schema_arrow.names)
    _require_signal_columns(names, kind="Parquet file")
    columns = [COL_TIME]
    if COL_VAL in names:
        columns.append(COL_VAL)
    if COL_VAL_TEXT in names:
        columns.append(COL_VAL_TEXT)
    for batch in pf.iter_batches(batch_size=batch_size, columns=columns):
        yield pa.Table.from_batches([batch])


def plan_uploads(signals: Sequence[SignalUpload]) -> list[PlannedUpload]:
    if not signals:
        raise ValueError("signals must not be empty")
    if len(signals) > MAX_SIGNALS_PER_ADD:
        raise ValueError(f"add_signals accepts at most {MAX_SIGNALS_PER_ADD} signals per call")

    planned: list[PlannedUpload] = []
    for upload in signals:
        total_rows, times = _inspect_source(upload)
        planned.append(
            PlannedUpload(
                upload=upload,
                row_counts=plan_file_rows(total_rows),
                frequency=estimate_frequency(times),
            )
        )
    return planned


def write_signal_parquets(
    planned: PlannedUpload,
    dest_dir: Path,
    dataset_id: int,
    signal_id: int,
    *,
    row_group_size: int = ROW_GROUP_SIZE,
    max_rows_per_file: int = MAX_ROWS_PER_FILE,
) -> list[WrittenParquet]:
    writer = LakeParquetWriter(
        dest_dir,
        dataset_id,
        signal_id,
        row_group_size=row_group_size,
        max_rows_per_file=max_rows_per_file,
        prefix=f"signal_{signal_id}",
    )
    for table in _iter_source_tables(planned.upload, batch_size=row_group_size):
        writer.write_table(table)
    files = writer.close()
    actual_rows = [f.rows for f in files]
    if actual_rows != planned.row_counts:
        raise RuntimeError(
            f"Wrote row counts {actual_rows} for signal {planned.upload.name!r}, expected {planned.row_counts}"
        )
    return files


def _validate_presign_response(response: requests.Response, failure_message: str) -> list[dict[str, Any]]:
    if response.status_code == 409:
        try:
            body = response.json()
        except ValueError:
            body = {}
        raise SignalsAlreadyExistError(
            body.get("signals") or [],
            message=f"{failure_message}: {body.get('error', 'signals_already_exist')}",
        )
    result = validate_response(response, failure_message)
    if not isinstance(result, list):
        raise ValueError(f"{failure_message}: expected a list response")
    return result


def _put_presigned(client: DBClient, url: str, path: Path, size: int) -> None:
    with path.open("rb") as file:
        response = client.storage_session.put(
            url,
            data=file,
            headers={"Content-Length": str(size)},
        )
    validate_storage_response(response, "Storage PUT failed")


def run_signal_uploads(
    client: DBClient,
    stream_id: int,
    dataset_id: int,
    planned: list[PlannedUpload],
    *,
    concurrency: int = 4,
) -> list[int]:
    """Presign, write lake parquet, PUT, and complete. Returns allocated signal IDs."""
    body = {
        "signals": [
            {
                "name": p.upload.name,
                "metadata": p.upload.metadata,
                "files": [{"index": i, "rows": rows} for i, rows in enumerate(p.row_counts)],
                "overwrite": p.upload.overwrite,
            }
            for p in planned
        ]
    }
    r = client.post(f"/stream/{stream_id}/dataset/{dataset_id}/signal/uploads", json=body)
    presigned = _validate_presign_response(r, "Presign signal uploads failed")

    if len(presigned) != len(planned):
        raise ValueError(f"Presign returned {len(presigned)} signals, expected {len(planned)}")

    signal_ids = [int(item["signal_id"]) for item in presigned]
    put_jobs: list[tuple[str, Path, int]] = []
    complete_payload: list[dict[str, Any]] = []

    with tempfile.TemporaryDirectory(prefix="mdb-signal-upload-") as tmp:
        tmp_dir = Path(tmp)
        for plan, response in zip(planned, presigned):
            signal_id = int(response["signal_id"])
            signal_dir = tmp_dir / f"signal_{signal_id}"
            signal_dir.mkdir()
            written = write_signal_parquets(plan, signal_dir, dataset_id, signal_id)
            remote_files = sorted(response["files"], key=lambda f: f["index"])
            if len(written) != len(remote_files):
                raise ValueError(
                    f"Signal {plan.upload.name!r}: wrote {len(written)} files but got {len(remote_files)} URLs"
                )
            file_meta = []
            for local, remote in zip(written, remote_files):
                put_jobs.append((remote["url"], local.local_path, local.size))
                file_meta.append({"path": remote["path"], "size": local.size, "footer": local.footer})
            complete_payload.append(
                {
                    "id": signal_id,
                    "priority": plan.upload.priority,
                    "stats": {"sum": None, "mean": None, "frequency": plan.frequency},
                    "files": file_meta,
                }
            )

        workers = max(concurrency, 1)
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(_put_presigned, client, url, path, size) for url, path, size in put_jobs]
            for future in as_completed(futures):
                future.result()

        complete = client.post(
            f"/stream/{stream_id}/dataset/{dataset_id}/signal/uploads/complete",
            json={"signals": complete_payload},
        )
        validate_response(complete, "Complete signal uploads failed")

    return signal_ids


def coerce_signal_uploads(signals: Sequence[SignalUpload | dict[str, Any]]) -> list[SignalUpload]:
    result: list[SignalUpload] = []
    for item in signals:
        if isinstance(item, SignalUpload):
            result.append(item)
        else:
            result.append(SignalUpload.model_validate(item))
    return result
