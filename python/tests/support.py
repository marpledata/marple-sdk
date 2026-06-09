from datetime import datetime
from pathlib import Path

import marple
from marple.db import Dataset, DataStream

EXAMPLE_CSV = Path(__file__).parents[2] / "test_data" / "examples_race.csv"


def ingest_dataset(stream: DataStream, metadata: dict | None = None) -> Dataset:
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
