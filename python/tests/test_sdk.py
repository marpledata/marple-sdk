import os
import random
import time
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

import marple
import pyarrow.parquet as pq
import pytest
from h5py import File
from marple import DB, Insight

EXAMPLE_CSV = Path(__file__).parent / "examples_race.csv"


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
def stream_name(db: DB) -> str:
    name = "Salty Compulsory Pytest " + datetime.now().isoformat()
    stream_id = db.create_stream(name)
    yield name
    db.delete_stream(stream_id)


@pytest.fixture(scope="session")
def dataset_id(db: DB, stream_name: str) -> int:
    file_name = f"pytest-sdk-{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.csv"
    dataset_id = db.push_file(
        stream_name,
        str(EXAMPLE_CSV),
        metadata={
            "source": "pytest:test_db.py",
            "sdk_version": marple.__version__,
        },
        file_name=file_name,
    )
    assert isinstance(dataset_id, int)

    deadline = time.monotonic() + 10

    last_status: dict | None = None
    while time.monotonic() < deadline:
        last_status = db.get_status(stream_name, dataset_id)
        import_status = last_status.get("import_status")
        if import_status in {"FINISHED", "FAILED"}:
            break
        time.sleep(0.5)

    assert last_status is not None, "No status returned while polling ingest status."
    assert last_status.get("import_status") == "FINISHED", f"Ingest did not finish: {last_status}"
    yield dataset_id


def test_db_check_connection(db: DB) -> None:
    assert db.check_connection() is True


def test_db_get_streams_and_datasets(db: DB, stream_name: str) -> None:
    streams = db.get_streams()["streams"]
    assert stream_name in [stream["name"] for stream in streams]

    datasets = db.get_datasets(stream_name)
    assert isinstance(datasets, list)


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
        paths = db.download_signal(stream_name, dataset_id, signal["id"], destination_folder=tmp_path)
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
