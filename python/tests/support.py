from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from uuid import uuid4

import marple
from marple import DB
from marple.db import Dataset, DataStream

EXAMPLE_CSV = Path(__file__).parents[2] / "test_data" / "examples_race.csv"

_swept_prefixes: set[str] = set()


def unique_name(prefix: str, suffix: str = "") -> str:
    """Datapool-unique name so parallel Python/Rust CI jobs cannot collide."""
    return f"{prefix}-{uuid4().hex}{suffix}"


def sweep_streams_by_prefix(db: DB, prefix: str) -> None:
    """Delete leftover streams with this prefix once per process."""
    if prefix in _swept_prefixes:
        return
    _swept_prefixes.add(prefix)
    for stream in db.get_streams():
        if stream.name.startswith(prefix):
            try:
                db.delete_stream(stream.id)
            except Exception:
                pass


@contextmanager
def isolated_stream(db: DB, prefix: str, suffix: str, **create_kwargs) -> Iterator[DataStream]:
    sweep_streams_by_prefix(db, prefix)
    stream = db.create_stream(f"{prefix} {suffix} {datetime.now().isoformat()}", **create_kwargs)
    try:
        yield stream
    finally:
        try:
            db.delete_stream(stream.id)
        except Exception:
            pass


def ingest_dataset(stream: DataStream, metadata: dict | None = None) -> Dataset:
    dataset = stream.push_file(
        str(EXAMPLE_CSV),
        metadata={
            "source": "pytest:test_db.py",
            "sdk_version": marple.__version__,
        }
        | (metadata or {}),
        file_name=unique_name("py-sdk", ".csv"),
    ).wait_for_import(timeout=180)
    assert (
        dataset.import_status == "FINISHED"
    ), f"Dataset {dataset.id} did not finish importing (status: {dataset.import_status})"
    return dataset
