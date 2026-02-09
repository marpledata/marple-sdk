import os
import random
import re
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
    url = os.getenv("MDB_API_URL", marple.db.SAAS_URL)
    assert url is not None
    new_db = DB(_required_env("MDB_TOKEN"), url)
    print(f"DB ID: {id(new_db)}")
    return new_db


@pytest.fixture(scope="session")
def insight() -> Insight:
    url = os.getenv("INSIGHT_API_URL", marple.insight.SAAS_URL)
    assert url is not None
    return Insight(_required_env("INSIGHT_TOKEN"), api_url=url)


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


def wait_for_ingestion(
    db: DB, stream_name, dataset_ids: list[int], timeout: float = 20, allow_iceberg=False
) -> None:
    finished_statuses = ["FINISHED", "FAILED"]
    if allow_iceberg:
        finished_statuses.append("UPDATING_ICEBERG")
    start = time.monotonic()
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
    print(f"Waited for {time.monotonic() - start:.1f}s for ingestion to finish. Last statuses: {last_statuses}")
    sucess_statuses = ["FINISHED"]
    if allow_iceberg:
        sucess_statuses.append("UPDATING_ICEBERG")
    for dataset_id, status in last_statuses.items():
        assert status != {}, "No status returned while polling ingest status."
        assert status.get("import_status") in sucess_statuses, f"Ingest did not finish: {status}"


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
    wait_for_ingestion(db, stream_name, dataset_ids=ids, timeout=30, allow_iceberg=True)

    # all_datasets = db.get_datasets(stream_name)
    stream = db.get_stream(stream_name)
    all_datasets = stream.get_datasets()
    assert len(all_datasets) == len(ids)

    datasets_a1 = all_datasets.where_metadata({"A": 1})
    assert len(datasets_a1) == 2

    datasets_b23 = all_datasets.where_metadata({"B": [2, 3]})
    assert len(datasets_b23) == 2

    assert len(all_datasets.where_dataset("hot_bytes", equals=0)) == 3
    assert len(all_datasets.where_dataset("cold_bytes", less_than=1000)) == 0
    assert len(all_datasets.where_dataset("cold_bytes", greater_than=1000)) == len(ids)
    assert len(all_datasets.where_dataset("created_at", greater_than=time.time() - 1000)) == len(ids)
    assert len(all_datasets.where_dataset("n_datapoints", equals=15 * 12500)) == len(ids)
    assert len(all_datasets.where_dataset("timestamp_start", equals=int(0.1 * 1e9))) == len(ids)

    def test_signal_filter(signal_name: str, stat, value: float) -> None:
        assert len(all_datasets.where_signal(signal_name, stat, equals=value)) == len(
            ids
        ), f"Failed on {signal_name} {stat} == {value}, stat in datasets: {[d.get_signal(signal_name).stats.get(stat) for d in all_datasets]}"
        assert len(all_datasets.where_signal(signal_name, stat, greater_than=value)) == 0
        assert len(all_datasets.where_signal(signal_name, stat, greater_than=value - 1)) == len(ids)
        assert len(all_datasets.where_signal(signal_name, stat, less_than=value)) == 0
        assert len(all_datasets.where_signal(signal_name, stat, less_than=value + 1)) == len(ids)

    random_dataset = random.choice(all_datasets)
    possible_names = [
        "car.speed",
        "car.dist",
        "car.lap.num",
        "car.engine.NGear",
        "car.engine.trq",
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

    assert len(all_datasets.where_predicate(custom_filter)) == 3


def test_get_signals(db: DB, stream_name: str, dataset_id: int) -> None:
    dataset = db.get_dataset(stream_name, dataset_id)

    assert len(dataset.get_signals()) == 15
    assert len(dataset.get_signals(signal_names=["car.speed", "car.accel", "some_random_signal"])) == 2
    assert len(dataset.get_signals(signal_names=[re.compile(r"car\.wheel\..*")])) == 4
    assert len(dataset.get_signals(signal_names=["car.speed", re.compile(r"car\.wheel.*")])) == 5
    assert len(dataset.get_signals(signal_names=["car\.wheel.*"])) == 0


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
            timestamp_stop=int(1e9),
            destination=tmp_path,
        )
        assert Path(file_path).exists()
        with File(file_path, "r") as f:
            assert "car.speed" in f
            assert "car.accel" not in f
            assert len(f["car.speed"]["time"][:]) == 10
