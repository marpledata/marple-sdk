import os
import random
from pathlib import Path

import pytest

from marple import DB, Insight

pytestmark = pytest.mark.integration


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        pytest.skip(f"Missing env var {name}; skipping integration test.")
    return value


def test_insight_export_mdb(tmp_path: Path) -> None:
    """
    Integration test for exporting a dataset via Insight.

    Requires:
    - MDB_TOKEN
    - INSIGHT_TOKEN
    Optional:
    - INSIGHT_STREAM_NAME (defaults to "Compulsory Salty Vaccine")
    - INSIGHT_EXPORT_FORMAT (defaults to "h5")
    """
    db = DB(_required_env("MDB_TOKEN"))
    insight = Insight(_required_env("INSIGHT_TOKEN"))

    stream_name = os.getenv("INSIGHT_STREAM_NAME", "Compulsory Salty Vaccine")
    export_format = os.getenv("INSIGHT_EXPORT_FORMAT", "h5")

    datasets = db.get_datasets(stream_name)
    if not datasets:
        pytest.skip(f"No datasets found in stream {stream_name!r}.")

    dataset = random.choice(datasets)
    stream_id = db._stream_name_to_id(stream_name)
    insight.export_mdb(stream_id, dataset["id"], format=export_format, destination=str(tmp_path))

    exported = tmp_path / f"export.{export_format}"
    assert exported.exists()
