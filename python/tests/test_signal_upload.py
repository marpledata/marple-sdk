"""Unit and integration tests for Dataset.add_signals."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from marple import DB
from marple.db import Dataset, SignalsAlreadyExistError, SignalUpload
from marple.db.constants import (
    COL_TIME,
    COL_VAL,
    COL_VAL_TEXT,
    LAKE_PARQUET_SCHEMA,
    MAX_SIGNALS_PER_ADD,
)
from marple.db.signal_upload import (
    LakeParquetWriter,
    estimate_frequency,
    parquet_footer_size,
    plan_file_rows,
)
from support import ingest_dataset

SIGNAL_UPLOAD_TEST_PREFIX = "Salty Compulsory PytestSignalUpload"


@pytest.fixture()
def require_signal_upload_api(db: DB) -> None:
    """Skip when the Marple DB deployment does not expose signal upload endpoints."""
    response = db.client.post("/stream/0/dataset/0/signal/uploads", json={"signals": []})
    if response.status_code == 405:
        pytest.skip("Signal upload API not available on this Marple DB deployment")


def test_plan_file_rows() -> None:
    assert plan_file_rows(100) == [100]
    assert plan_file_rows(100, max_rows_per_file=40) == [40, 40, 20]
    with pytest.raises(ValueError):
        plan_file_rows(0)


def test_estimate_frequency() -> None:
    times = pd.array([0, 1_000_000_000, 2_000_000_000], dtype="int64").to_numpy()
    assert estimate_frequency(times) == pytest.approx(1.0)
    assert estimate_frequency(times[:1]) == 1.0


def test_lake_parquet_writer_and_footer(tmp_path: Path) -> None:
    writer = LakeParquetWriter(
        tmp_path,
        dataset_id=5,
        signal_id=7,
        row_group_size=3,
        max_rows_per_file=5,
        prefix="test",
    )
    table = pa.table(
        {
            COL_TIME: [10, 20, 30, 40, 50, 60, 70],
            COL_VAL: [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0],
        }
    )
    writer.write_table(table)
    files = writer.close()

    assert [f.rows for f in files] == [5, 2]
    assert plan_file_rows(7, max_rows_per_file=5) == [5, 2]

    for written in files:
        assert written.footer == parquet_footer_size(written.local_path)
        metadata = pq.read_metadata(written.local_path)
        schema = metadata.schema.to_arrow_schema()
        assert schema.equals(LAKE_PARQUET_SCHEMA)
        assert metadata.num_row_groups >= 1
        # dataset/signal identity stats present
        for col_name, expected in (("dataset", 5), ("signal", 7)):
            idx = schema.get_field_index(col_name)
            for rg in range(metadata.num_row_groups):
                stats = metadata.row_group(rg).column(idx).statistics
                assert stats is not None and stats.has_min_max
                assert stats.min == expected
                assert stats.max == expected


def test_signal_upload_requires_one_source() -> None:
    with pytest.raises(ValueError, match="exactly one"):
        SignalUpload(name="x")
    with pytest.raises(ValueError, match="exactly one"):
        SignalUpload(name="x", data=pd.DataFrame({COL_TIME: [1], COL_VAL: [1.0]}), path="a.parquet")


def test_add_signals_rejects_empty_and_too_many() -> None:
    from marple.db.signal_upload import coerce_signal_uploads, plan_uploads

    with pytest.raises(ValueError, match="empty"):
        plan_uploads([])

    too_many = coerce_signal_uploads(
        [
            {"name": f"s{i}", "data": pd.DataFrame({COL_TIME: [1], COL_VAL: [float(i)]})}
            for i in range(MAX_SIGNALS_PER_ADD + 1)
        ]
    )
    with pytest.raises(ValueError, match=str(MAX_SIGNALS_PER_ADD)):
        plan_uploads(too_many)


def _cleanup_signal_upload_streams(db: DB) -> None:
    for stream in db.get_streams():
        if stream.name.startswith(SIGNAL_UPLOAD_TEST_PREFIX):
            db.delete_stream(stream.id)


@contextmanager
def _signal_upload_stream(db: DB, suffix: str):
    _cleanup_signal_upload_streams(db)
    stream = db.create_stream(
        f"{SIGNAL_UPLOAD_TEST_PREFIX} {suffix} {datetime.now().isoformat()}",
        plugin_args="--use-index",
    )
    try:
        yield stream
    finally:
        _cleanup_signal_upload_streams(db)


def _times_in_dataset_range(dataset: Dataset, n: int = 5) -> list[int]:
    assert dataset.timestamp_start is not None
    assert dataset.timestamp_stop is not None
    start = int(dataset.timestamp_start)
    stop = int(dataset.timestamp_stop)
    if n == 1:
        return [start]
    step = max((stop - start) // (n - 1), 1)
    return [start + i * step for i in range(n)]


def test_add_signals_dataframe_happy_path(db: DB, require_signal_upload_api: None) -> None:
    with _signal_upload_stream(db, "df") as stream:
        dataset = ingest_dataset(stream, metadata={"test": "add_signals_df"})
        times = _times_in_dataset_range(dataset)
        df = pd.DataFrame({COL_TIME: times, COL_VAL: [float(i) for i in range(len(times))]})

        signals = dataset.add_signals(
            [{"name": "sdk.derived_speed", "data": df, "metadata": {"unit": "m/s"}}],
            wait=True,
            timeout=120,
        )

        assert len(signals) == 1
        signal = signals[0]
        assert signal.name == "sdk.derived_speed"
        assert signal.storage_status == "COLD"
        assert signal.count == len(times)
        assert signal.unit == "m/s"

        data = signal.get_data(refresh_cache=True)
        assert len(data) == len(times)
        assert list(data.index.asi8) == times


def test_add_signals_path_and_overwrite(db: DB, require_signal_upload_api: None) -> None:
    with _signal_upload_stream(db, "path") as stream:
        dataset = ingest_dataset(stream, metadata={"test": "add_signals_path"})
        times = _times_in_dataset_range(dataset, n=4)
        df = pd.DataFrame(
            {
                COL_TIME: times,
                COL_VAL: [1.0, None, 3.0, 4.0],
                COL_VAL_TEXT: [None, "hello", None, None],
            }
        )

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "signal.parquet"
            pq.write_table(pa.Table.from_pandas(df, preserve_index=False), path)

            first = dataset.add_signals(
                [SignalUpload(name="sdk.from_path", path=path)],
                wait=True,
                timeout=120,
            )
            assert first[0].storage_status == "COLD"
            first_id = first[0].id

            with pytest.raises(SignalsAlreadyExistError) as exc:
                dataset.add_signals([{"name": "sdk.from_path", "path": path}], wait=False)
            assert any(s.get("status") == "EXISTS" for s in exc.value.signals)

            df2 = pd.DataFrame({COL_TIME: times, COL_VAL: [10.0, 20.0, 30.0, 40.0]})
            path2 = Path(tmp) / "signal2.parquet"
            pq.write_table(pa.Table.from_pandas(df2, preserve_index=False), path2)

            second = dataset.add_signals(
                [{"name": "sdk.from_path", "path": path2, "overwrite": True}],
                wait=True,
                timeout=120,
            )
            assert second[0].id == first_id
            assert second[0].storage_status == "COLD"
            assert second[0].count == 4


def test_add_signals_duplicate_names_in_batch(db: DB, require_signal_upload_api: None) -> None:
    with _signal_upload_stream(db, "dup") as stream:
        dataset = ingest_dataset(stream, metadata={"test": "add_signals_dup"})
        times = _times_in_dataset_range(dataset, n=3)
        df = pd.DataFrame({COL_TIME: times, COL_VAL: [1.0, 2.0, 3.0]})

        with pytest.raises(SignalsAlreadyExistError) as exc:
            dataset.add_signals(
                [
                    {"name": "sdk.dup", "data": df},
                    {"name": "sdk.dup", "data": df},
                ],
                wait=False,
            )
        assert any(s.get("status") == "DUPLICATE" for s in exc.value.signals)
