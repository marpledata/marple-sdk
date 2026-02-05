import os
import random
import time
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Generator

import dotenv
import marple
import pandas as pd
import pyarrow.parquet as pq
import pytest
from h5py import File
from marple import DB, Insight

EXAMPLE_CSV = Path(__file__).parent / "examples_race.csv"


dotenv.load_dotenv()


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        pytest.fail(f"Missing env var {name}; skipping integration test.")
    return value


@pytest.fixture(scope="session")
def db() -> DB:
    return DB(_required_env("MDB_TOKEN"))


@pytest.fixture(scope="session")
def insight() -> Insight:
    return Insight(_required_env("INSIGHT_TOKEN"))


@pytest.fixture(scope="session")
def stream_name(db: DB) -> Generator[str, None, None]:
    name = "Salty Compulsory Pytest " + datetime.now().isoformat()
    stream_id = db.create_stream(name)
    yield name
    db.delete_stream(stream_id)


@pytest.fixture(scope="session")
def dataset_id(db: DB, stream_name: str, metadata: dict | None = None) -> Generator[int, None, None]:
    dataset_id = ingest_dataset(db, stream_name, metadata=metadata)
    assert isinstance(dataset_id, int)

    wait_for_ingestion(db, stream_name, dataset_ids=[dataset_id])

    yield dataset_id


def ingest_dataset(db: DB, stream_name: str, metadata: dict | None = None) -> int:
    file_name = f"pytest-sdk-{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.csv"
    return db.push_file(
        stream_name,
        str(EXAMPLE_CSV),
        metadata={
            "source": "pytest:test_db.py",
            "sdk_version": marple.__version__,
        }
        | (metadata or {}),
        file_name=file_name,
    )


def wait_for_ingestion(db: DB, stream_name, dataset_ids: list[int], timeout: float = 20) -> None:
    finished_statuses = ["FINISHED", "FAILED", "UPDATING_ICEBERG"]
    deadline = time.monotonic() + timeout

    last_statuses: dict[int, dict] = {id: {} for id in dataset_ids}
    while time.monotonic() < deadline:
        for dataset_id in dataset_ids:
            last_statuses[dataset_id] = db.get_status(stream_name, dataset_id)
        if all(
            last_statuses[dataset_id].get("import_status") in finished_statuses for dataset_id in dataset_ids
        ):
            break
        time.sleep(0.5)

    for dataset_id in dataset_ids:
        status = last_statuses[dataset_id]
        assert len(status) > 0, "No status returned while polling ingest status."
        assert status.get("import_status") in [
            "FINISHED",
            "UPDATING_ICEBERG",
        ], f"Ingest did not finish: {status}"


def test_db_check_connection(db: DB) -> None:
    assert db.check_connection() is True


def test_db_get_streams_and_datasets(db: DB, stream_name: str) -> None:
    streams = db.get_streams()
    assert stream_name in [stream.name for stream in streams]

    datasets = db.get_datasets(stream_name)
    assert isinstance(datasets, marple.db.DatasetList)


def test_db_filter_datasets(db: DB, stream_name: str) -> None:
    id1 = ingest_dataset(db, stream_name, metadata={"A": 1, "B": 1})
    id2 = ingest_dataset(db, stream_name, metadata={"A": 1, "B": 2})
    id3 = ingest_dataset(db, stream_name, metadata={"A": 4, "B": 3})
    ids = [id1, id2, id3]
    wait_for_ingestion(db, stream_name, dataset_ids=ids, timeout=30)

    all_datasets = db.get_datasets(stream_name)
    assert len(all_datasets) == len(ids)

    datasets_a1 = all_datasets.where_metadata({"A": 1})
    assert len(datasets_a1) == 2

    datasets_b23 = all_datasets.where_metadata({"B": [2, 3]})
    assert len(datasets_b23) == 2

    random_dataset = random.choice(all_datasets)
    random_signal = random.choice(random_dataset.get_signals())

    def test_signal_filter(signal_name: str, stat, value: float) -> None:
        assert len(all_datasets.where_signal(signal_name, stat, equals=value)) == len(ids)
        assert len(all_datasets.where_signal(signal_name, stat, greater_than=value)) == 0
        assert len(all_datasets.where_signal(signal_name, stat, greater_than=value - 1)) == len(ids)
        assert len(all_datasets.where_signal(signal_name, stat, less_than=value)) == 0
        assert len(all_datasets.where_signal(signal_name, stat, less_than=value + 1)) == len(ids)

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


def test_db_query_endpoint(db: DB) -> None:
    query = "select path, stream_id, metadata from mdb_default_dataset limit 1;"
    response = db.post("/query", json={"query": query})
    assert response.status_code == 200
    assert response.json() is not None


def test_db_get_original(db: DB, stream_name: str, dataset_id: int) -> None:
    with TemporaryDirectory() as tmp_path:
        file_path = db.download_original(stream_name, dataset_id, destination_folder=tmp_path)
        p = Path(file_path)
        assert p.exists()
        assert p.stat().st_size == EXAMPLE_CSV.stat().st_size


def test_db_get_parquet(db: DB, stream_name: str, dataset_id: int) -> None:
    signals = db.get_signals(stream_name, dataset_id)
    signal = random.choice(signals)
    with TemporaryDirectory() as tmp_path:
        paths = db.download_signal(stream_name, dataset_id, signal.id, destination_folder=tmp_path)
        assert len(paths) > 0
        for path in paths:
            table = pq.read_table(path)
            assert table is not None
            assert "time" in table.column_names
            assert "value" in table.column_names
            assert "value_text" in table.column_names


@pytest.fixture()
def insight_dataset(insight: Insight, dataset_id: int):
    yield insight.get_dataset_mdb(dataset_id)


def test_insight_mdb_signals(insight: Insight, dataset_id: int) -> None:
    signals = insight.get_signals_mdb(dataset_id)
    assert len(signals) > 0
    assert "car.speed" in [signal["name"] for signal in signals]
    assert "car.accel" in [signal["name"] for signal in signals]


def test_insight_export(insight: Insight, insight_dataset: dict) -> None:
    with TemporaryDirectory() as tmp_path:
        file_path = insight.export_data(
            insight_dataset["dataset_filter"],
            format="h5",
            signals=["car.speed"],
            timestamp_stop=1e9,
            destination=tmp_path,
        )
        assert Path(file_path).exists()
        with File(file_path, "r") as f:
            assert "car.speed" in f
            assert "car.accel" not in f
            assert len(f["car.speed"]["time"][:]) == 10
