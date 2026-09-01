from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from marple import DB
from marple.db import Dataset, DataStream, Script
from support import ingest_dataset, isolated_stream, unique_name

SCRIPTS_TEST_PREFIX = "Salty Compulsory PytestScripts"

SPEED_KMH_SCRIPT = """
from marple.db import Dataset

def process(dataset: Dataset) -> None:
    speed = dataset.get_signal("car.speed")
    if speed is None:
        return
    dataset.add_signal("car.speed_kmh", speed.get_data() * 3.6, metadata={"unit": "km/h"})
"""

PASS_SCRIPT = "def process(dataset):\n    pass\n"


def _ok(body: Any) -> SimpleNamespace:
    return SimpleNamespace(status_code=200, json=lambda: body)


def _stream_payload(**overrides: Any) -> dict[str, Any]:
    payload = {
        "type": "files",
        "id": 7,
        "name": "Car data",
        "description": None,
        "datapool": "default",
        "layer_shifts": [1000],
        "version_id": 1,
        "created_at": 0.0,
        "last_updated": 0.0,
        "scripts": [],
    }
    payload.update(overrides)
    return payload


def _script_payload(**overrides: Any) -> dict[str, Any]:
    payload = {
        "id": 3,
        "name": "speed_kmh",
        "description": "Derive km/h",
        "created_at": 1.0,
        "created_by": "sdk",
        "updated_at": 2.0,
        "updated_by": "sdk",
        "streams": [7],
        "versions": [
            {
                "id": 10,
                "script": "print('ok')",
                "updated_at": 2.0,
                "updated_by": "sdk",
            }
        ],
    }
    payload.update(overrides)
    return payload


def _dataset_payload(**overrides: Any) -> dict[str, Any]:
    payload = {
        "id": 42,
        "stream_id": 7,
        "datastream_version": 1,
        "created_at": 0.0,
        "created_by": "sdk",
        "import_status": "POSTPROCESSING",
        "import_progress": 0.0,
        "import_message": None,
        "import_time": None,
        "path": "lap.csv",
        "metadata": {},
        "cold_path": "",
        "cold_bytes": 0,
        "hot_bytes": 0,
        "backup_path": None,
        "backup_size": None,
        "plugin": "csv",
        "plugin_args": None,
        "n_datapoints": 0,
        "n_signals": 0,
        "timestamp_start": None,
        "timestamp_stop": None,
        "import_speed": None,
        "parquet_version": 1,
    }
    payload.update(overrides)
    return payload


@pytest.fixture(scope="session")
def require_scripts_api(db: DB) -> None:
    response = db.client.get("/scripts")
    if response.status_code in (404, 405):
        pytest.skip("Processing scripts API not available on this Marple DB deployment")


@pytest.fixture(scope="session")
def require_rerun_processing_api(db: DB) -> None:
    response = db.client.post("/stream/0/processing/datasets", json=[1])
    if response.status_code in (404, 405):
        pytest.skip("Rerun processing API not available on this Marple DB deployment")


def test_stream_defaults_scripts_when_missing() -> None:
    stream = DataStream(client=MagicMock(), **{k: v for k, v in _stream_payload().items() if k != "scripts"})
    assert stream.scripts == []


def test_stream_update_posts_only_set_fields() -> None:
    client = MagicMock()
    client.post.return_value = _ok({"status": "success"})
    client.get.return_value = _ok(_stream_payload(description="updated", scripts=[1, 2]))
    stream = DataStream(client=client, **_stream_payload())

    updated = stream.update(description="updated", scripts=[1, 2])

    client.post.assert_called_once_with(
        "/stream/update/7",
        json={"description": "updated", "scripts": [1, 2]},
    )
    client.get.assert_called_once_with("/stream/7")
    assert updated.description == "updated"
    assert updated.scripts == [1, 2]


def test_stream_rerun_processing_posts_dataset_ids() -> None:
    client = MagicMock()
    client.post.return_value = _ok({"status": "success"})
    stream = DataStream(client=client, **_stream_payload())

    stream.rerun_processing([42, 43])

    client.post.assert_called_once_with("/stream/7/processing/datasets", json=[42, 43])


def test_stream_rerun_processing_whole_stream() -> None:
    client = MagicMock()
    client.post.return_value = _ok({"status": "success"})
    stream = DataStream(client=client, **_stream_payload())

    stream.rerun_processing()

    client.post.assert_called_once_with("/stream/7/processing")


def test_stream_rerun_processing_rejects_empty_ids() -> None:
    stream = DataStream(client=MagicMock(), **_stream_payload())
    with pytest.raises(ValueError, match="at least one dataset id"):
        stream.rerun_processing([])


def test_dataset_rerun_processing_posts_and_fetches() -> None:
    client = MagicMock()
    client.datapool = "default"
    client.post.return_value = _ok({"status": "success"})
    client.get.return_value = _ok(_dataset_payload())
    dataset = Dataset(client=client, **_dataset_payload(import_status="FINISHED"))

    updated = dataset.rerun_processing()

    client.post.assert_called_once_with("/stream/7/processing/datasets", json=[42])
    assert updated.import_status == "POSTPROCESSING"


def test_dataset_reingest_and_debug_messages() -> None:
    client = MagicMock()
    client.datapool = "default"
    client.post.return_value = _ok({"status": "success"})
    client.get.side_effect = [
        _ok(_dataset_payload(import_status="WAITING")),
        _ok(["Rerunning processing..."]),
    ]
    dataset = Dataset(client=client, **_dataset_payload(import_status="FINISHED"))

    updated = dataset.reingest()
    messages = dataset.get_debug_messages()

    client.post.assert_called_once_with("/stream/7/dataset/42/reingest")
    assert updated.import_status == "WAITING"
    assert messages == ["Rerunning processing..."]


def test_resolve_source_from_text() -> None:
    assert Script.resolve_source(PASS_SCRIPT) == PASS_SCRIPT


def test_resolve_source_from_py_string(tmp_path: Path) -> None:
    path = tmp_path / "speed.py"
    path.write_text(PASS_SCRIPT)
    assert Script.resolve_source(str(path)) == PASS_SCRIPT


def test_resolve_source_from_path_object(tmp_path: Path) -> None:
    path = tmp_path / "speed"
    path.write_text(PASS_SCRIPT)
    assert Script.resolve_source(path) == PASS_SCRIPT


def test_resolve_source_rejects_missing_process() -> None:
    with pytest.raises(ValueError, match="exactly one process"):
        Script.resolve_source("print('ok')")


def test_script_source_and_update() -> None:
    client = MagicMock()
    client.post.return_value = _ok(_script_payload(description="Metadata only"))
    script = Script(client=client, **_script_payload())

    assert script.source == "print('ok')"
    updated = script.update(description="Metadata only")

    client.post.assert_called_once_with("/script/3", json={"description": "Metadata only"})
    assert updated.description == "Metadata only"


def test_script_update_posts_resolved_source() -> None:
    client = MagicMock()
    client.post.return_value = _ok(_script_payload())
    script = Script(client=client, **_script_payload())

    script.update(script=PASS_SCRIPT)

    client.post.assert_called_once_with("/script/3", json={"script": PASS_SCRIPT})


def test_script_delete_and_duplicate() -> None:
    client = MagicMock()
    client.delete.return_value = _ok({"status": "success", "id": 3})
    client.post.return_value = _ok(_script_payload(id=4, name="speed_kmh (Copy)"))
    script = Script(client=client, **_script_payload())

    copy = script.duplicate()
    script.delete()

    client.post.assert_called_once_with("/script/3/duplicate")
    client.delete.assert_called_once_with("/script/3")
    assert copy.id == 4
    assert copy.name == "speed_kmh (Copy)"


@pytest.mark.integration
def test_script_crud_and_stream_pipeline(db: DB, require_scripts_api: None) -> None:
    name = unique_name("py-sdk-script")
    with isolated_stream(db, SCRIPTS_TEST_PREFIX, "pipeline", plugin_args="--use-index") as stream:
        script = db.create_script(
            name,
            SPEED_KMH_SCRIPT,
            description="Created by SDK test",
            streams=[stream.id],
        )
        duplicate = None
        try:
            assert script.name == name
            assert script.streams == [stream.id]
            assert script.source is not None and "def process" in script.source
            assert any(s.id == script.id for s in db.get_scripts())
            assert db.get_script(script.id).id == script.id
            assert db.get_stream(stream.id).scripts == [script.id]

            updated = script.update(description="Metadata only")
            assert updated.description == "Metadata only"
            assert len(updated.versions) == 1

            coded = updated.update(script=PASS_SCRIPT)
            assert coded.source == PASS_SCRIPT
            assert len(coded.versions) == 2

            duplicate = coded.duplicate()
            assert duplicate.name == f"{name} (Copy)"
            assert duplicate.streams == [stream.id]

            stream = stream.update(scripts=[script.id, duplicate.id])
            assert stream.scripts == [script.id, duplicate.id]

            stream = stream.update(description="SDK processing test")
            assert stream.description == "SDK processing test"
            assert stream.scripts == [script.id, duplicate.id]

            stream = stream.update(scripts=[])
            assert stream.scripts == []
        finally:
            if duplicate is not None:
                duplicate.delete()
            db.delete_script(script.id)


@pytest.mark.integration
def test_rerun_processing_and_debug(db: DB, require_rerun_processing_api: None) -> None:
    with isolated_stream(db, SCRIPTS_TEST_PREFIX, "rerun", plugin_args="--use-index") as stream:
        dataset = ingest_dataset(stream)
        messages = dataset.get_debug_messages()
        assert isinstance(messages, list)

        rerun = dataset.rerun_processing().wait_for_import(timeout=180)
        assert rerun.import_status == "FINISHED"
        assert rerun.id == dataset.id

        stream.rerun_processing([dataset.id])
        finished = dataset.wait_for_import(timeout=180, force_fetch=True)
        assert finished.import_status == "FINISHED"
