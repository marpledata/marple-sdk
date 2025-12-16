import os
import shutil
import time
from math import sin, tau
from pathlib import Path

import pytest

from marple import Marple

pytestmark = pytest.mark.integration


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        pytest.skip(f"Missing env var {name}; skipping integration test.")
    return value


def test_files_upload_and_send_data(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Integration test for the legacy Marple Files API client (`Marple`).

    Requires:
    - MARPLE_ACCESS_TOKEN
    Optional:
    - MARPLE_FOLDER (defaults to "/")
    - MARPLE_PROJECT (defaults to "api-project")
    """
    access_token = _required_env("MARPLE_ACCESS_TOKEN")
    marple_folder = os.getenv("MARPLE_FOLDER", "/")
    project_name = os.getenv("MARPLE_PROJECT", "api-project")

    m = Marple(access_token)
    assert m.check_connection() is True

    example_csv = Path(__file__).parent / "examples_race.csv"
    assert example_csv.exists()

    file_copy = tmp_path / f"example_{int(time.time())}.csv"
    shutil.copy(example_csv, file_copy)

    source_id = m.upload_data_file(
        str(file_copy),
        marple_folder=marple_folder,
        metadata={"source": "pytest:test_files.py", "type": "data file"},
    )
    assert source_id

    link = m.get_link(source_id, project_name, open_link=False)
    assert isinstance(link, str) and link

    # `send_data` writes a temporary CSV in the current working directory.
    monkeypatch.chdir(tmp_path)
    for i in range(0, 5):
        m.add_data({"time": i, "signal 1": i, "signal 2": sin(i / tau)})
    source_id_2 = m.send_data(f"pytest_add_data_{int(time.time())}", marple_folder=marple_folder)
    assert source_id_2
