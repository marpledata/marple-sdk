import os
import random
import re
import time
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

import dotenv
import marple
import pandas as pd
import pyarrow.parquet as pq
import pytest
from h5py import File
from marple import DB, Insight
from marple.db import Dataset, DataStream
from marple.db.constants import SCHEMA
from requests import HTTPError

EXAMPLE_CSV = Path(__file__).parent / "examples_race.csv"


dotenv.load_dotenv()


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None:
        pytest.fail(f"Missing env var {name}; skipping integration test.")
    return value


@pytest.fixture()
def db() -> DB:
    url = os.getenv("MDB_API_URL", marple.db.SAAS_URL)
    assert url is not None
    return DB(_required_env("MDB_TOKEN"), url)


@pytest.fixture(scope="session")
def insight() -> Insight:
    url = os.getenv("INSIGHT_API_URL", marple.insight.SAAS_URL)
    assert url is not None
    return Insight(_required_env("INSIGHT_TOKEN"), api_url=url)


def _ingest_dataset(stream: DataStream, metadata: dict | None = None) -> Dataset:
    file_name = f"pytest-sdk-{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.csv"
    return stream.push_file(
        str(EXAMPLE_CSV),
        metadata={
            "source": "pytest:test_db.py",
            "sdk_version": marple.__version__,
        }
        | (metadata or {}),
        file_name=file_name,
    ).wait_for_import()


@pytest.fixture(scope="session")
def example_stream() -> DataStream:
    url = os.getenv("MDB_API_URL", marple.db.SAAS_URL)
    assert url is not None
    session_db = DB(_required_env("MDB_TOKEN"), url)

    name = "Salty Compulsory Pytest " + datetime.now().isoformat()
    yield session_db.create_stream(name)
    print("Cleaning up stream...")
    session_db.delete_stream(name)  # optional cleanup


@pytest.fixture()
def example_dataset(example_stream: DataStream) -> Dataset:
    return _ingest_dataset(example_stream, metadata={"A": 1, "B": 1})


def test_db_check_connection(db: DB) -> None:
    assert db.check_connection() is True
    with pytest.raises(Exception, match="Invalid API token"):
        DB("invalid_token", marple.db.SAAS_URL).check_connection()



def test_db_get_streams_and_datasets(db: DB, example_stream: DataStream) -> None:
    streams = db.get_streams()
    assert example_stream.name in [stream.name for stream in streams]

    datasets = example_stream.get_datasets()
    assert isinstance(datasets, marple.db.DatasetList)


def test_db_filter_datasets(example_stream: DataStream) -> None:
    n_datasets = 3
    dataset_1 = _ingest_dataset(example_stream, metadata={"A": 1, "B": 1})
    _ingest_dataset(example_stream, metadata={"A": 1, "B": 2})
    _ingest_dataset(example_stream, metadata={"A": 4, "B": 3})
    all_datasets = example_stream.get_datasets()

    datasets_a1 = all_datasets.where_metadata({"A": 1})
    assert len(datasets_a1) == 2

    datasets_b23 = all_datasets.where_metadata({"B": [2, 3]})
    assert len(datasets_b23) == 2

    assert dataset_1.id == all_datasets.where_metadata({"A": 1, "B": 1})[0].id

    assert len(all_datasets.where_dataset("hot_bytes", equals=0)) == n_datasets
    assert len(all_datasets.where_dataset("cold_bytes", less_than=1000)) == 0
    assert len(all_datasets.where_dataset("cold_bytes", greater_than=1000)) == n_datasets
    assert len(all_datasets.where_dataset("created_at", greater_than=time.time() - 1000)) == n_datasets
    assert len(all_datasets.where_dataset("n_datapoints", equals=15 * 12500)) == n_datasets
    assert len(all_datasets.where_dataset("timestamp_start", equals=int(0.1 * 1e9))) == n_datasets

    def test_signal_filter(signal_name: str, stat, value: float) -> None:
        assert (
            len(all_datasets.where_signal(signal_name, stat, equals=value)) == n_datasets
        ), f"Failed on {signal_name} {stat} == {value}, stat in datasets: {[d.get_signal(signal_name).stats.get(stat) for d in all_datasets]}"
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

    df = pd.read_csv(EXAMPLE_CSV)
    actual_signal = df[random_signal.name]
    time_col = df["time"]
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
        return (
            dataset.metadata.get("A") == 1
            and dataset.metadata.get("B") in [2, 3]
            or dataset.get_signal("car.engine.NGear").stats.get("max", 0) ** 2 > 16
        )

    assert len(all_datasets.where(custom_filter)) == n_datasets


def test_get_data(example_stream: DataStream) -> None:
    datasets = example_stream.get_datasets()
    assert len(datasets) > 0
    dataset = random.choice(datasets)

    n_signals = 5
    random_signals = random.sample(dataset.get_signals(), k=n_signals)
    random_signal_names = [signal.name for signal in random_signals]
    all_data = dataset.get_data(signals=random_signal_names)
    assert isinstance(all_data, pd.DataFrame)
    assert set(all_data.columns) == set(random_signal_names)
    assert all_data.shape == (12500, n_signals)
    assert all_data.index.name == "time"
    assert isinstance(all_data.index, pd.TimedeltaIndex)
    assert all_data.index[1] - all_data.index[0] == pd.Timedelta(0.1, unit="s")

    datasets = datasets[:5]
    fewer_random_signal_names = random_signal_names[:2]
    for dataset, df in datasets.get_data(
        signals=fewer_random_signal_names, resample_rule="3.579s", resample_aggregate="max"
    ):
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
    assert len(example_dataset.get_signals(signal_names=["car\.wheel.*"])) == 0


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


def test_db_get_parquet(example_dataset: Dataset) -> None:
    signals = example_dataset.get_signals()
    signal = random.choice(signals)
    table = pq.ParquetDataset(signal.cache_parquet()).read()
    assert table.column_names == ["dataset", "signal", "time", "value", "value_text"]
    assert table.num_rows == 12500


@pytest.fixture()
def insight_dataset(insight: Insight, example_dataset: Dataset):
    yield insight.get_dataset_mdb(example_dataset.id)


def test_insight_mdb_signals(insight: Insight, example_dataset: Dataset) -> None:
    signals = insight.get_signals_mdb(example_dataset.id)
    assert len(signals) > 0
    assert "car.speed" in [signal["name"] for signal in signals]
    assert "car.accel" in [signal["name"] for signal in signals]


def test_insight_export(insight: Insight, insight_dataset: dict) -> None:
    with TemporaryDirectory() as tmp_path:
        file_path = insight.export_data(
            insight_dataset["dataset_filter"],
            format="h5",
            signals=["car.speed"],
            timestamp_stop=int(1e9),
            destination=tmp_path,
        )
        assert Path(file_path).exists()
        with File(file_path, "r") as f:
            assert "car.speed" in f
            assert "car.accel" not in f
            assert len(f["car.speed"]["time"][:]) == 10
