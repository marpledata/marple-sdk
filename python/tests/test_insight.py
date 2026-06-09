from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from h5py import File

from marple import Insight
from marple.db import Dataset


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
