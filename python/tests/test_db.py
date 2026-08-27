import random
import re
import time
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Literal

import pandas as pd
import pyarrow.parquet as pq
import pytest
from requests import HTTPError

import marple
from marple import DB
from marple.db import Dataset, DatasetList, DataStream
from support import EXAMPLE_CSV, TINY_CSV, ingest_datasets, isolated_stream

pytestmark = pytest.mark.integration

PYTHON_UPLOAD_TEST_PREFIX = "Salty Compulsory PytestUpload"
PYTHON_FILTER_TEST_PREFIX = "Salty Compulsory PytestFilter"
REALTIME_TEST_PREFIX = "Salty Compulsory PytestRealtime"
MIB = 1024 * 1024
MULTIPART_THRESHOLD = 128 * MIB


def _generate_multipart_blob(destination_folder: str) -> Path:
    destination = Path(destination_folder) / "multipart-blob.bin"
    with destination.open("wb") as f:
        f.write(b"multipart-upload-test")
        f.seek(MULTIPART_THRESHOLD)
        f.write(b"x")
    assert destination.stat().st_size > MULTIPART_THRESHOLD
    return destination


def _push_and_assert_upload(
    stream: DataStream,
    file_path: Path,
    metadata: dict[str, str],
    upload_mode: Literal["auto", "server"] = "auto",
    overwrite: bool = False,
    file_name: str | None = None,
    wait_for_import: bool = True,
) -> Dataset:
    dataset = stream.push_file(
        str(file_path), metadata=metadata, upload_mode=upload_mode, overwrite=overwrite, file_name=file_name
    )

    if wait_for_import:
        dataset = dataset.wait_for_import(timeout=180)
        assert dataset.import_status == "FINISHED"

    if dataset.backup_size is not None:
        assert dataset.backup_size == file_path.stat().st_size
    for key, value in metadata.items():
        assert dataset.metadata.get(key) == value

    if wait_for_import:
        with TemporaryDirectory() as tmp_path:
            downloaded = dataset.download(destination_folder=tmp_path)
            assert Path(downloaded).stat().st_size == file_path.stat().st_size
            assert Path(downloaded).stat().st_size == dataset.backup_size

    return dataset


def test_db_check_connection(db: DB) -> None:
    assert db.check_connection() is True
    with pytest.raises(Exception):
        DB("invalid_token", marple.db.SAAS_URL).check_connection()


def test_db_get_streams_and_datasets(db: DB, example_stream: DataStream) -> None:
    streams = db.get_streams()
    assert example_stream.name in [stream.name for stream in streams]

    datasets = example_stream.get_datasets()
    assert isinstance(datasets, marple.db.DatasetList)


def test_db_filter_datasets(db: DB) -> None:
    n_datasets = 3
    tiny_df = pd.read_csv(TINY_CSV)
    n_rows = len(tiny_df)
    n_signals = 15

    with isolated_stream(db, PYTHON_FILTER_TEST_PREFIX, "filter") as stream:
        ingested = ingest_datasets(
            stream,
            [{"A": 1, "B": 1}, {"A": 1, "B": 2}, {"A": 4, "B": 3}],
        )
        dataset_1 = ingested[0]
        all_datasets = stream.get_datasets()

        datasets_a1 = all_datasets.where_metadata({"A": 1})
        assert len(datasets_a1) == 2

        datasets_b23 = all_datasets.where_metadata({"B": [2, 3]})
        assert len(datasets_b23) == 2

        assert dataset_1.id == all_datasets.where_metadata({"A": 1, "B": 1})[0].id

        assert len(all_datasets.where_dataset("hot_bytes", equals=0)) == n_datasets
        assert len(all_datasets.where_dataset("cold_bytes", less_than=1)) == 0
        assert len(all_datasets.where_dataset("cold_bytes", greater_than=0)) == n_datasets
        assert len(all_datasets.where_dataset("created_at", greater_than=time.time() - 1000)) == n_datasets
        assert len(all_datasets.where_dataset("n_datapoints", equals=n_signals * n_rows)) == n_datasets
        assert len(all_datasets.where_dataset("timestamp_start", equals=int(0.1 * 1e9))) == n_datasets

        def test_signal_filter(signal_name: str, stat, value: float) -> None:
            assert len(all_datasets.where_signal(signal_name, stat, equals=value)) == n_datasets, (
                f"Failed on {signal_name} {stat} == {value}, stat in datasets: "
                f"{[(s.stats.get(stat) if (s := d.get_signal(signal_name)) and s.stats else None) for d in all_datasets]}"
            )
            assert len(all_datasets.where_signal(signal_name, stat, greater_than=value)) == 0
            assert len(all_datasets.where_signal(signal_name, stat, greater_than=value - 1)) == n_datasets
            assert len(all_datasets.where_signal(signal_name, stat, less_than=value)) == 0
            assert len(all_datasets.where_signal(signal_name, stat, less_than=value + 1)) == n_datasets

        random_dataset = random.choice(all_datasets)
        possible_names = [
            "car.speed",
            "car.dist",
            "car.lap.num",
            "car.engine.NGear",
            "car.engine.speed",
            "car.wheel.left.trq",
            "car.wheel.right.trq",
            "car.wheel.left.speed",
            "car.wheel.right.speed",
        ]  # Some signals fail due to rounding with the avg stat

        random_signal = random_dataset.get_signal(random.choice(possible_names))
        assert random_signal is not None

        actual_signal = tiny_df[random_signal.name]
        time_col = tiny_df["time"]
        test_signal_filter(random_signal.name, "min", actual_signal.min())
        test_signal_filter(random_signal.name, "max", actual_signal.max())
        test_signal_filter(random_signal.name, "mean", actual_signal.mean())
        test_signal_filter(random_signal.name, "sum", actual_signal.sum())
        test_signal_filter(random_signal.name, "frequency", 10)
        test_signal_filter(random_signal.name, "time_min", int(time_col.min() * 1e9))
        test_signal_filter(random_signal.name, "time_max", int(time_col.max() * 1e9))
        test_signal_filter(random_signal.name, "count", actual_signal.count())
        test_signal_filter(random_signal.name, "count_value", actual_signal.count())
        test_signal_filter(random_signal.name, "count_text", 0)

        def custom_filter(dataset: marple.db.Dataset) -> bool:
            ng_signal = dataset.get_signal("car.engine.NGear")
            assert ng_signal is not None and ng_signal.stats is not None
            return (
                dataset.metadata.get("A") == 1
                and dataset.metadata.get("B") in [2, 3]
                or ng_signal.stats.get("max", 0) ** 2 > 16
            )

        assert len(all_datasets.where(custom_filter)) == n_datasets


def test_get_data(example_dataset: Dataset) -> None:
    n_signals = 5
    random_signals = random.sample(example_dataset.get_signals(), k=n_signals)
    random_signal_names = [signal.name for signal in random_signals]
    all_data = example_dataset.get_data(signals=random_signal_names)
    assert isinstance(all_data, pd.DataFrame)
    assert set(all_data.columns) == set(random_signal_names)
    assert all_data.shape == (12500, n_signals)
    assert all_data.index.name == "time"
    assert isinstance(all_data.index, pd.TimedeltaIndex)
    assert all_data.index[1] - all_data.index[0] == pd.Timedelta(0.1, unit="s")

    fewer_random_signal_names = random_signal_names[:2]
    for dataset, df in DatasetList([example_dataset]).get_data(
        signals=fewer_random_signal_names, resample_rule="3.579s", resample_aggregate="max"
    ):
        assert dataset.id == example_dataset.id
        assert isinstance(df, pd.DataFrame)
        assert set(df.columns) == set(fewer_random_signal_names)
        assert df.shape[1] == 2
        assert df.index.name == "time"
        assert isinstance(df.index, pd.TimedeltaIndex)
        assert df.index[1] - df.index[0] == pd.Timedelta(3.579, unit="s")


def test_get_signals(example_dataset: Dataset) -> None:
    assert len(example_dataset.get_signals()) == 15
    assert len(example_dataset.get_signals(signal_names=["car.speed", "car.accel", "some_random_signal"])) == 2
    assert len(example_dataset.get_signals(signal_names=[re.compile(r"car\.wheel\..*")])) == 4
    assert len(example_dataset.get_signals(signal_names=["car.speed", re.compile(r"car\.wheel.*")])) == 5
    assert len(example_dataset.get_signals(signal_names=[r"car\.wheel.*"])) == 0


def test_test_dataset(db: DB, example_dataset: Dataset) -> None:

    assert example_dataset.n_signals == 15
    assert example_dataset.n_datapoints == 12500 * 15

    with pytest.raises(ValueError):
        db.get_dataset()

    with pytest.raises(ValueError):
        db.get_dataset(dataset_id=example_dataset.id, dataset_path="non.existent.path")

    with pytest.raises(HTTPError):
        db.get_dataset(dataset_id=-3)

    assert db.get_dataset(dataset_id=example_dataset.id).id == example_dataset.id


def test_get_signal(example_dataset: Dataset) -> None:
    signal = example_dataset.get_signal("car.speed")
    assert signal is not None
    assert signal.name == "car.speed"
    assert signal.count == 12500

    with pytest.raises(ValueError):
        example_dataset.get_signal("non.existent.signal")

    with pytest.raises(ValueError):
        example_dataset.get_signal(name="car.speed", id=signal.id)

    with pytest.raises(ValueError):
        example_dataset.get_signal()


def test_db_get_original(example_dataset: Dataset) -> None:
    with TemporaryDirectory() as tmp_path:
        file_path = example_dataset.download(destination_folder=tmp_path)
        p = Path(file_path)
        assert p.exists()
        assert p.stat().st_size == EXAMPLE_CSV.stat().st_size


def test_push_file_server_upload(db: DB) -> None:
    metadata = {"upload_mode": "server"}
    with isolated_stream(db, PYTHON_UPLOAD_TEST_PREFIX, "server", plugin_args="--use-index") as stream:
        dataset = _push_and_assert_upload(stream, TINY_CSV, metadata, upload_mode="server")

    assert dataset.metadata.get("upload_mode") == "server"


def test_push_file_overwrite(db: DB) -> None:
    metadata_v1 = {"version": "1"}
    metadata_v2 = {"version": "2"}
    overwrite_file_name = f"overwrite_test_{datetime.now().isoformat()}.csv"
    with isolated_stream(db, PYTHON_UPLOAD_TEST_PREFIX, "overwrite", plugin_args="--use-index") as stream:
        _push_and_assert_upload(stream, TINY_CSV, metadata_v1, file_name=overwrite_file_name)
        dataset_overwritten = _push_and_assert_upload(
            stream, TINY_CSV, metadata_v2, overwrite=True, file_name=overwrite_file_name
        )

        assert dataset_overwritten.import_status == "FINISHED"
        assert dataset_overwritten.metadata.get("version") == "2"
        # Ensure it actually overwrote instead of duplicating
        assert len(stream.get_datasets()) == 1


def test_push_file_multipart_upload(db: DB) -> None:
    metadata = {"upload_mode": "multipart"}
    with TemporaryDirectory() as tmp_path:
        multipart_blob = _generate_multipart_blob(tmp_path)
        with isolated_stream(db, PYTHON_UPLOAD_TEST_PREFIX, "multipart", plugin_args="--use-index") as stream:
            dataset = _push_and_assert_upload(stream, multipart_blob, metadata, wait_for_import=False)

    assert dataset.metadata.get("upload_mode") == "multipart"


def test_db_get_parquet(example_dataset: Dataset) -> None:
    signals = example_dataset.get_signals()
    signal = random.choice(signals)
    table = pq.ParquetDataset(signal.cache_parquet()).read()
    assert table.column_names == ["dataset", "signal", "time", "value", "value_text"]
    assert table.num_rows == 12500


def test_realtime_dataset_cool(db: DB) -> None:
    with isolated_stream(db, REALTIME_TEST_PREFIX, "cool", type="realtime") as stream:
        dataset = stream.add_dataset("pytest-run", metadata={"source": "pytest"})
        assert dataset.import_status == "LIVE"

        dataset.upsert_signals([{"signal": "speed", "unit": "km/h"}])
        dataset.append(
            pd.DataFrame(
                {
                    "time": [1_000_000_000, 2_000_000_000],
                    "signal": ["speed", "speed"],
                    "value": [10.0, 20.0],
                }
            ),
        )

        finished = dataset.cool().wait_for_import(timeout=180)
        assert finished.import_status == "FINISHED"
        assert finished.id == dataset.id


def test_realtime_cool_rejects_invalid_status(example_dataset: Dataset) -> None:
    with pytest.raises(ValueError, match="cannot be cooled"):
        example_dataset.cool()
