"""Unit and integration tests for Dataset.add_signal / add_signals."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from marple import DB
from marple.db import Dataset, DataStream, SignalsAlreadyExistError, SignalUpload
from marple.db.constants import (
    COL_TIME,
    COL_VAL,
    COL_VAL_TEXT,
    LAKE_PARQUET_SCHEMA,
    MAX_ROWS_PER_FILE,
    MAX_SIGNALS_PER_ADD,
    ROW_GROUP_SIZE,
)
from marple.db.signal_upload import (
    ParquetUploadMetadata,
    PresignedParquetFile,
    PresignedSignal,
    _estimate_frequency,
    _plan_row_counts,
    run_signal_uploads,
)
from support import isolated_stream, unique_name

SIGNAL_UPLOAD_TEST_PREFIX = "Salty Compulsory PytestSignalUpload"


@pytest.fixture(scope="session")
def require_signal_upload_api(db: DB) -> None:
    """Skip when the Marple DB deployment does not expose signal upload endpoints."""
    response = db.client.post("/stream/0/dataset/0/signal/uploads", json={"signals": []})
    if response.status_code in (404, 405):
        pytest.skip("Signal upload API not available on this Marple DB deployment")


@pytest.fixture(scope="session")
def require_signal_delete_api(db: DB, require_signal_upload_api: None) -> None:
    probe = db.client.post("/stream/0/dataset/0/signals/delete", json={"signal_ids": []})
    if probe.status_code in (404, 405):
        pytest.skip("Signal delete API not available on this Marple DB deployment")


@pytest.fixture(scope="module")
def signal_upload_stream(db: DB, require_signal_upload_api: None):
    with isolated_stream(db, SIGNAL_UPLOAD_TEST_PREFIX, "shared", plugin_args="--use-index") as stream:
        yield stream


def _fake_dataset(
    *,
    dataset_id: int = 1,
    timestamp_start: int = 0,
    timestamp_stop: int = 10_000_000_000,
) -> Dataset:
    client = MagicMock()
    client.post.return_value = SimpleNamespace(status_code=200, json=lambda: {"status": "success"})
    return cast(
        Dataset,
        SimpleNamespace(
            id=dataset_id,
            datastream_id=99,
            timestamp_start=timestamp_start,
            timestamp_stop=timestamp_stop,
            _client=client,
        ),
    )


def _patch_upload_io(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> list[Path]:
    """Stub presign + storage PUT; capture written parquet files for inspection."""
    captured: list[Path] = []

    def fake_presign(dataset: Any, planned: list, *, overwrite: bool = False) -> dict[str, PresignedSignal]:
        return {
            plan.name: PresignedSignal(
                name=plan.name,
                signal_id=7,
                files=[
                    PresignedParquetFile(
                        index=i,
                        rows=rows,
                        path=f"lake/part_{i}.parquet",
                        url="https://example.test/upload",
                        expires_in=60,
                    )
                    for i, rows in enumerate(plan.row_counts)
                ],
            )
            for plan in planned
        }

    def fake_upload(_client: Any, remote: PresignedParquetFile, local_path: Path) -> ParquetUploadMetadata:
        dest = tmp_path / f"part_{remote.index}.parquet"
        dest.write_bytes(local_path.read_bytes())
        captured.append(dest)
        with local_path.open("rb") as file:
            file.seek(-8, 2)
            footer = int.from_bytes(file.read(4), "little")
        return ParquetUploadMetadata(path=remote.path, size=local_path.stat().st_size, footer=footer)

    monkeypatch.setattr("marple.db.signal_upload._presign_signals", fake_presign)
    monkeypatch.setattr("marple.db.signal_upload._upload_parquet", fake_upload)
    return captured


def test_plan_row_counts() -> None:
    assert _plan_row_counts(100) == [100]
    assert _plan_row_counts(MAX_ROWS_PER_FILE) == [MAX_ROWS_PER_FILE]
    assert _plan_row_counts(MAX_ROWS_PER_FILE + 1) == [MAX_ROWS_PER_FILE, 1]
    assert _plan_row_counts(2 * MAX_ROWS_PER_FILE + 7) == [MAX_ROWS_PER_FILE, MAX_ROWS_PER_FILE, 7]


def test_estimate_frequency() -> None:
    times = pa.array([0, 1_000_000_000, 2_000_000_000], type=pa.int64())
    assert _estimate_frequency(times) == pytest.approx(1.0)
    assert _estimate_frequency(times.slice(0, 1)) is None


def test_run_signal_upload_writes_lake_parquet(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Exercise real write path with network stubbed; assert lake parquet quality."""
    captured = _patch_upload_io(monkeypatch, tmp_path)
    dataset = _fake_dataset(dataset_id=5)
    n = ROW_GROUP_SIZE + 10
    df = pd.DataFrame(
        {
            COL_TIME: list(range(n)),
            COL_VAL: [1.0] * n,
        }
    )

    ids = run_signal_uploads(dataset, [SignalUpload(name="sdk.test", data=df)])
    assert ids == [7]
    assert len(captured) == 1

    path = captured[0]
    metadata = pq.read_metadata(path)
    schema = metadata.schema.to_arrow_schema()
    assert schema.equals(LAKE_PARQUET_SCHEMA)
    assert metadata.num_rows == n
    assert metadata.num_row_groups == 2
    assert metadata.row_group(0).num_rows == ROW_GROUP_SIZE
    assert metadata.row_group(1).num_rows == 10

    for col_name, expected in (("dataset", 5), ("signal", 7)):
        idx = schema.get_field_index(col_name)
        for rg in range(metadata.num_row_groups):
            stats = metadata.row_group(rg).column(idx).statistics
            assert stats is not None and stats.has_min_max
            assert stats.min == expected
            assert stats.max == expected

    # Complete payload should include a positive parquet footer length.
    complete_body = dataset._client.post.call_args.kwargs["json"]  # type: ignore[attr-defined]
    assert complete_body["signals"][0]["files"][0]["footer"] > 0


def test_run_signal_upload_splits_files(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    captured = _patch_upload_io(monkeypatch, tmp_path)
    monkeypatch.setattr("marple.db.signal_upload.MAX_ROWS_PER_FILE", 5)
    dataset = _fake_dataset()
    df = pd.DataFrame({COL_TIME: list(range(12)), COL_VAL: [float(i) for i in range(12)]})

    run_signal_uploads(dataset, [{"name": "sdk.split", "data": df}])
    assert [pq.read_metadata(p).num_rows for p in captured] == [5, 5, 2]


def test_signal_upload_validation_errors() -> None:
    dataset = _fake_dataset()

    with pytest.raises(ValueError, match="at least one row"):
        SignalUpload(name="empty", data=pd.DataFrame({COL_TIME: [], COL_VAL: []})).plan_upload(dataset)

    with pytest.raises(ValueError, match="time"):
        SignalUpload(name="no_time", data=pd.DataFrame({COL_VAL: [1.0]})).plan_upload(dataset)

    with pytest.raises(ValueError, match="value"):
        SignalUpload(name="no_value", data=pd.DataFrame({COL_TIME: [1]})).plan_upload(dataset)

    with pytest.raises(ValueError, match="nulls"):
        SignalUpload(
            name="null_time",
            data=pd.DataFrame({COL_TIME: [1, None], COL_VAL: [1.0, 2.0]}),
        ).plan_upload(dataset)

    with pytest.raises(ValueError, match="greater than or equal to 0"):
        SignalUpload(
            name="neg_time",
            data=pd.DataFrame({COL_TIME: [-1], COL_VAL: [1.0]}),
        ).plan_upload(dataset)

    with pytest.raises(ValueError, match="does not overlap"):
        SignalUpload(
            name="oob",
            data=pd.DataFrame({COL_TIME: [20_000_000_000], COL_VAL: [1.0]}),
        ).plan_upload(dataset)


def test_signal_upload_non_finite_becomes_null() -> None:
    dataset = _fake_dataset()
    planned = SignalUpload(
        name="nan",
        data=pd.DataFrame({COL_TIME: [0, 1], COL_VAL: [1.0, float("nan")]}),
    ).plan_upload(dataset)
    values = planned.data.column(COL_VAL).to_pylist()
    assert values[0] == 1.0
    assert values[1] is None


def test_signal_upload_value_text_only() -> None:
    dataset = _fake_dataset()
    planned = SignalUpload(
        name="text",
        data=pd.DataFrame({COL_TIME: [0, 1], COL_VAL_TEXT: ["a", "b"]}),
    ).plan_upload(dataset)
    assert planned.data.column(COL_VAL).null_count == 2
    assert planned.data.column(COL_VAL_TEXT).to_pylist() == ["a", "b"]


def test_signal_upload_from_time_indexed_series() -> None:
    dataset = _fake_dataset()
    series = pd.Series([1.0, 2.0], index=pd.to_datetime([0, 1_000_000_000], unit="ns"))
    planned = SignalUpload(name="series", data=series).plan_upload(dataset)
    assert planned.data.column(COL_TIME).to_pylist() == [0, 1_000_000_000]
    assert planned.data.column(COL_VAL).to_pylist() == [1.0, 2.0]
    assert planned.data.column(COL_VAL_TEXT).null_count == 2


def test_signal_upload_from_time_indexed_frame() -> None:
    """A one-column frame straight out of Signal.get_data round-trips without rebuilding."""
    dataset = _fake_dataset()
    index = pd.to_timedelta([0, 1_000_000_000], unit="ns")
    frame = pd.DataFrame({COL_VAL: [1.0, 2.0]}, index=index)
    planned = SignalUpload(name="frame", data=frame * 3.6).plan_upload(dataset)
    assert planned.data.column(COL_TIME).to_pylist() == [0, 1_000_000_000]
    assert planned.data.column(COL_VAL).to_pylist() == [3.6, 7.2]


def test_signal_upload_time_column_wins_over_index() -> None:
    dataset = _fake_dataset()
    frame = pd.DataFrame(
        {COL_TIME: [0, 1_000_000_000], COL_VAL: [1.0, 2.0]},
        index=pd.to_datetime([5_000_000_000, 6_000_000_000], unit="ns"),
    )
    planned = SignalUpload(name="both", data=frame).plan_upload(dataset)
    assert planned.data.column(COL_TIME).to_pylist() == [0, 1_000_000_000]


def test_signal_upload_text_series_becomes_value_text() -> None:
    dataset = _fake_dataset()
    series = pd.Series(["a", "b"], index=pd.to_timedelta([0, 1_000_000_000], unit="ns"))
    planned = SignalUpload(name="text_series", data=series).plan_upload(dataset)
    assert planned.data.column(COL_VAL_TEXT).to_pylist() == ["a", "b"]
    assert planned.data.column(COL_VAL).null_count == 2


def test_signal_upload_ignores_unrelated_columns() -> None:
    dataset = _fake_dataset()
    frame = pd.DataFrame(
        {COL_VAL: [1.0, 2.0], "label": ["a", "b"], "other": [3, 4]},
        index=pd.to_timedelta([0, 1_000_000_000], unit="ns"),
    )
    planned = SignalUpload(name="extra_cols", data=frame).plan_upload(dataset)
    assert planned.data.schema.names == [COL_TIME, COL_VAL, COL_VAL_TEXT]
    assert planned.data.column(COL_VAL).to_pylist() == [1.0, 2.0]
    assert planned.data.column(COL_VAL_TEXT).null_count == 2


def test_signal_upload_normalizes_index_resolution() -> None:
    """A coarser index resolution must not be bit-cast to int64 as if it were nanoseconds."""
    dataset = _fake_dataset()
    index = pd.to_timedelta([0, 1_000_000_000], unit="ns").as_unit("us")
    planned = SignalUpload(name="micros", data=pd.Series([1.0, 2.0], index=index)).plan_upload(dataset)
    assert planned.data.column(COL_TIME).to_pylist() == [0, 1_000_000_000]


def test_signal_upload_tz_aware_index_uses_utc() -> None:
    dataset = _fake_dataset()
    index = pd.to_datetime([0, 1_000_000_000], unit="ns").tz_localize("UTC").tz_convert("Europe/Brussels")
    planned = SignalUpload(name="tz", data=pd.Series([1.0, 2.0], index=index)).plan_upload(dataset)
    assert planned.data.column(COL_TIME).to_pylist() == [0, 1_000_000_000]


def test_signal_upload_rejects_nat_index() -> None:
    dataset = _fake_dataset()
    series = pd.Series([1.0, 2.0], index=pd.DatetimeIndex([pd.Timestamp(0), pd.NaT]))
    with pytest.raises(ValueError, match="NaT"):
        SignalUpload(name="nat", data=series).plan_upload(dataset)


def test_signal_upload_rejects_non_time_index() -> None:
    dataset = _fake_dataset()
    with pytest.raises(ValueError, match="DatetimeIndex"):
        SignalUpload(name="range_series", data=pd.Series([1.0, 2.0])).plan_upload(dataset)

    with pytest.raises(ValueError, match="DatetimeIndex"):
        SignalUpload(name="range_frame", data=pd.DataFrame({COL_VAL: [1.0]})).plan_upload(dataset)


def test_planned_upload_to_request_omits_overwrite() -> None:
    dataset = _fake_dataset()
    planned = SignalUpload(
        name="x",
        data=pd.DataFrame({COL_TIME: [0, 1], COL_VAL: [1.0, 2.0]}),
        metadata={"unit": "m/s"},
    ).plan_upload(dataset)
    req = planned.to_request()
    assert "overwrite" not in req
    assert req["name"] == "x"
    assert req["metadata"] == {"unit": "m/s"}
    assert req["files"] == [{"index": 0, "rows": 2}]


def test_add_signals_rejects_too_many() -> None:
    too_many = [{"name": f"s{i}", "data": "unused"} for i in range(MAX_SIGNALS_PER_ADD + 1)]
    dataset = object.__new__(Dataset)
    with pytest.raises(ValueError, match=str(MAX_SIGNALS_PER_ADD)):
        Dataset.add_signals(dataset, too_many)


def test_add_signals_empty_returns_empty() -> None:
    dataset = object.__new__(Dataset)
    assert Dataset.add_signals(dataset, []) == []


def test_delete_signals_empty_is_noop() -> None:
    dataset = SimpleNamespace(_client=MagicMock(), _signals={}, n_signals=0)
    Dataset.delete_signals(cast(Dataset, dataset), [])
    dataset._client.post.assert_not_called()


def test_delete_signals_posts_and_clears_cache() -> None:
    client = MagicMock()
    client.post.return_value = SimpleNamespace(status_code=200, json=lambda: {"status": "success"})
    dataset = SimpleNamespace(
        id=10,
        datastream_id=5,
        n_signals=3,
        _client=client,
        _signals={1: MagicMock(), 2: MagicMock(), 3: MagicMock()},
    )

    Dataset.delete_signals(cast(Dataset, dataset), [1, 2])

    client.post.assert_called_once_with(
        "/stream/5/dataset/10/signals/delete",
        json={"signal_ids": [1, 2]},
    )
    assert set(dataset._signals) == {3}
    assert dataset.n_signals == 1


def test_signals_already_exist_error_message() -> None:
    err = SignalsAlreadyExistError(
        [{"name": "a", "status": "EXISTS"}, {"name": "b", "status": "DUPLICATE"}],
        message="Presign failed",
    )
    assert "EXISTS" in str(err)
    assert "DUPLICATE" in str(err)
    assert len(err.signals) == 2


@pytest.mark.integration
def test_add_signal_from_existing(signal_upload_stream: DataStream) -> None:
    dataset = signal_upload_stream.add_dataset(
        unique_name("py-sig"), metadata={"test": "add_signal_from_existing"}
    )
    t0 = 1_700_000_000_000_000_000
    speed_df = pd.DataFrame({COL_TIME: [t0, t0 + 1_000_000_000], COL_VAL: [1.0, 2.0]})
    speed = dataset.add_signal("car.speed", speed_df).wait_until_available(timeout=180)
    derived = speed.get_data() * 3.6
    signal = dataset.add_signal(
        "sdk.car.speed_kmh",
        derived,
        metadata={"unit": "km/h"},
    ).wait_until_available(timeout=180)

    assert signal.name == "sdk.car.speed_kmh"
    assert signal.storage_status == "COLD"
    assert signal.count == len(derived)
    assert signal.metadata.get("unit") == "km/h" or signal.unit == "km/h"

    by_name = dataset.get_signal("sdk.car.speed_kmh")
    assert by_name is not None
    assert by_name.id == signal.id

    data = signal.get_data(refresh_cache=True)
    assert len(data) == len(derived)
    pd.testing.assert_series_equal(
        data["value"].reset_index(drop=True),
        derived[COL_VAL].reset_index(drop=True),
        check_names=False,
        rtol=1e-9,
    )


@pytest.mark.integration
def test_add_signals_overwrite_and_conflict(signal_upload_stream: DataStream) -> None:
    dataset = signal_upload_stream.add_dataset(unique_name("py-sig"), metadata={"test": "add_signals_overwrite"})
    t0 = 1_700_000_000_000_000_000
    times = [t0, t0 + 1_000_000_000]
    df = pd.DataFrame({COL_TIME: times, COL_VAL: [1.0, 2.0]})

    first = dataset.add_signal("sdk.from_df", df).wait_until_available(timeout=180)
    first_id = first.id

    with pytest.raises(SignalsAlreadyExistError) as exc:
        dataset.add_signals([{"name": "sdk.from_df", "data": df}])
    assert any(s.get("status") == "EXISTS" for s in exc.value.signals)

    df2 = pd.DataFrame({COL_TIME: times, COL_VAL: [10.0, 20.0]})
    second = dataset.add_signal("sdk.from_df", df2, overwrite=True).wait_until_available(timeout=180)
    assert second.id == first_id
    assert second.storage_status == "COLD"
    data = second.get_data(refresh_cache=True)
    assert list(data["value"]) == [10.0, 20.0]


@pytest.mark.integration
def test_add_dataset_then_add_signals(signal_upload_stream: DataStream) -> None:
    """Custom file-stream ingest: empty dataset via add_dataset, then add_signals."""
    dataset = signal_upload_stream.add_dataset(
        unique_name("py-sig"), metadata={"test": "add_dataset_add_signals"}
    )
    t0 = 1_700_000_000_000_000_000
    df_a = pd.DataFrame({COL_TIME: [t0, t0 + 1_000_000_000], COL_VAL: [1.0, 2.0]})
    df_b = pd.DataFrame({COL_TIME: [t0, t0 + 1_000_000_000], COL_VAL: [3.0, 4.0]})

    ids = dataset.add_signals(
        [
            {"name": "sdk.custom.a", "data": df_a, "metadata": {"unit": "1"}},
            {"name": "sdk.custom.b", "data": df_b},
        ],
        concurrency=2,
    )
    assert len(ids) == 2

    signals = dataset.get_signals(signal_ids=ids, refresh=True)
    assert [s.id for s in signals] == ids
    for signal in signals:
        signal.wait_until_available(timeout=180)

    a = dataset.get_signal("sdk.custom.a", refresh=True)
    b = dataset.get_signal("sdk.custom.b", refresh=True)
    assert a is not None and b is not None
    assert a.storage_status == "COLD"
    assert b.storage_status == "COLD"
    assert list(a.get_data(refresh_cache=True)["value"]) == [1.0, 2.0]
    assert list(b.get_data(refresh_cache=True)["value"]) == [3.0, 4.0]


@pytest.mark.integration
def test_delete_signals_integration(signal_upload_stream: DataStream, require_signal_delete_api: None) -> None:
    dataset = signal_upload_stream.add_dataset(unique_name("py-sig"), metadata={"test": "delete_signals"})
    t0 = 1_700_000_000_000_000_000
    df_a = pd.DataFrame({COL_TIME: [t0, t0 + 1_000_000_000], COL_VAL: [1.0, 2.0]})
    df_b = pd.DataFrame({COL_TIME: [t0, t0 + 1_000_000_000], COL_VAL: [3.0, 4.0]})
    df_c = pd.DataFrame({COL_TIME: [t0, t0 + 1_000_000_000], COL_VAL: [5.0, 6.0]})

    ids = dataset.add_signals(
        [
            {"name": "sdk.delete.a", "data": df_a},
            {"name": "sdk.delete.b", "data": df_b},
            {"name": "sdk.delete.c", "data": df_c},
        ]
    )
    assert len(ids) == 3
    for signal in dataset.get_signals(signal_ids=ids, refresh=True):
        signal.wait_until_available(timeout=180)

    a = dataset.get_signal("sdk.delete.a", refresh=True)
    assert a is not None
    a.delete()
    assert dataset.get_signal("sdk.delete.a", refresh=True) is None

    dataset.delete_signals(ids[1:])
    remaining = dataset.get_signals(refresh=True)
    assert {s.name for s in remaining} == set()
