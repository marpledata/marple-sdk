import os
import time
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

import marple
import pytest
from marple import DB

STREAM_NAME = "Salty Compulsory Pytest"


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        pytest.skip(f"Missing env var {name}; skipping integration test.")
    return value


@pytest.fixture(scope="session")
def db() -> DB:
    return DB(_required_env("MDB_TOKEN"))


def test_db_check_connection(db: DB) -> None:
    assert db.check_connection() is True


def test_db_get_streams_and_datasets(db: DB) -> None:
    streams = db.get_streams()["streams"]
    assert STREAM_NAME in [stream["name"] for stream in streams]

    datasets = db.get_datasets(STREAM_NAME)
    assert isinstance(datasets, list)


def test_db_query_endpoint(db: DB) -> None:
    query = "select path, stream_id, metadata from mdb_default_dataset limit 1;"
    response = db.post("/query", json={"query": query})
    assert response.status_code == 200
    assert response.json() is not None


def test_db_upload_status_and_download(db: DB) -> None:
    example_csv = Path(__file__).parent / "examples_race.csv"
    assert example_csv.exists()

    file_name = f"pytest-sdk-{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.csv"
    dataset_id = db.push_file(
        STREAM_NAME,
        str(example_csv),
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
        last_status = db.get_status(STREAM_NAME, dataset_id)
        import_status = last_status.get("import_status")
        if import_status in {"FINISHED", "FAILED"}:
            break
        time.sleep(0.5)

    assert last_status is not None, "No status returned while polling ingest status."
    assert last_status.get("import_status") == "FINISHED", f"Ingest did not finish: {last_status}"

    with TemporaryDirectory() as tmp_path:
        file_path = db.download_original(STREAM_NAME, dataset_id, destination=tmp_path)
        p = Path(file_path)
        assert p.exists()
        assert p.stat().st_size == example_csv.stat().st_size
