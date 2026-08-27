from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from uuid import uuid4

import marple
from marple import DB
from marple.db import Dataset, DataStream

EXAMPLE_CSV = Path(__file__).parents[2] / "test_data" / "examples_race.csv"
TINY_CSV = Path(__file__).parents[2] / "test_data" / "tiny_race.csv"

_swept_prefixes: set[str] = set()


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


def _push_file(stream: DataStream, file_path: Path, metadata: dict | None) -> Dataset:
    return stream.push_file(
        str(file_path),
        metadata={
            "source": "pytest:test_db.py",
            "sdk_version": marple.__version__,
        }
        | (metadata or {}),
        file_name=f"pytest-sdk-{uuid4().hex}.csv",
    )


def _wait_for_finished(dataset: Dataset) -> Dataset:
    dataset = dataset.wait_for_import(timeout=180)
    assert (
        dataset.import_status == "FINISHED"
    ), f"Dataset {dataset.id} did not finish importing (status: {dataset.import_status})"
    return dataset


def ingest_dataset(stream: DataStream, metadata: dict | None = None, file_path: Path | None = None) -> Dataset:
    return _wait_for_finished(_push_file(stream, file_path or EXAMPLE_CSV, metadata))


def ingest_datasets(
    stream: DataStream,
    metadatas: list[dict],
    file_path: Path | None = None,
) -> list[Dataset]:
    """Push several files, then wait for import concurrently."""
    path = file_path or TINY_CSV
    pending = [_push_file(stream, path, metadata) for metadata in metadatas]
    with ThreadPoolExecutor(max_workers=max(len(pending), 1)) as pool:
        return list(pool.map(_wait_for_finished, pending))
