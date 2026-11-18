import csv
import io
from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest

from marple import DB
from marple.db import AliasingRule, DataStream, Signal, aliasing
from support import (
    ingest_dataset,
    isolated_stream,
    ok,
    poll,
    require_admin_api,
    unique_name,
)

ALIASING_TEST_PREFIX = "Salty Compulsory PytestAliasing"


def _rule_payload(**overrides: Any) -> dict[str, Any]:
    payload = {
        "id": 4,
        "signal_name": "car.speed",
        "signal_hierarchy": ["VehSpd", "veh_speed_kph"],
        "description": "Vehicle speed",
        "streams": [7],
        "copy_metadata": True,
        "created_by": "sdk@example.com",
        "updated_at": 1.5,
    }
    payload.update(overrides)
    return payload


def _client(*rules: dict[str, Any]) -> MagicMock:
    client = MagicMock()
    client.datapool = "default"
    client.get.return_value = ok(list(rules))
    return client


def _uploaded_rows(client: MagicMock) -> list[dict[str, str]]:
    name, content, content_type = client.post.call_args.kwargs["files"]["file"]
    assert name.endswith(".csv") and content_type == "text/csv"
    return list(csv.DictReader(io.StringIO(content.decode())))


def test_get_aliasing_rules_filters_by_signal_and_stream() -> None:
    produces = _rule_payload(id=1)
    consumes = _rule_payload(id=2, signal_name="speed", signal_hierarchy=["car.speed"], streams=[])
    other_stream = _rule_payload(id=3, streams=[8])
    # Rules uploaded as CSV in the UI can come back with null streams and copy_metadata
    unrelated = _rule_payload(
        id=4, signal_name="rpm", signal_hierarchy=["EngSpd"], streams=None, copy_metadata=None
    )
    client = _client(produces, consumes, other_stream, unrelated)

    assert [r.id for r in aliasing.list_rules(client)] == [1, 2, 3, 4]
    assert [r.id for r in aliasing.list_rules(client, "car.speed")] == [1, 2, 3]
    assert [r.id for r in aliasing.list_rules(client, "VehSpd", stream=7)] == [1]
    assert [r.id for r in aliasing.list_rules(client, stream=7)] == [1, 2, 4]
    client.get.assert_called_with("/aliasing")

    signal = MagicMock(spec=Signal)
    signal.name = "EngSpd"
    assert [r.id for r in aliasing.list_rules(client, signal)] == [4]


def test_signal_get_aliasing_rules_is_scoped_to_its_stream() -> None:
    client = _client(_rule_payload(id=1), _rule_payload(id=3, streams=[8]))
    signal = Signal.model_construct(name="car.speed", datastream_id=8)
    signal._client = client

    assert [r.id for r in signal.get_aliasing_rules()] == [3]

    signal.metadata = {}
    assert signal.alias_of is None
    signal.metadata = {"_mdb_reference_signal": "VehSpd"}
    assert signal.alias_of == "VehSpd"


def test_create_aliasing_rule() -> None:
    client = _client()
    client.post.return_value = ok({"status": "success", "rule": _rule_payload()})

    rule = aliasing.create_rule(client, "car.speed", ["VehSpd"], streams=[7], copy_metadata=True)

    client.post.assert_called_once_with(
        "/aliasing",
        json={
            "signal_name": "car.speed",
            "signal_hierarchy": ["VehSpd"],
            "description": None,
            "streams": [7],
            "copy_metadata": True,
        },
    )
    assert rule.id == 4 and rule.applies_to(7) and not rule.applies_to(8)
    with pytest.raises(TypeError, match="not a single string"):
        aliasing.create_rule(client, "car.speed", "VehSpd")  # type: ignore[arg-type]


def test_update_sends_merged_full_rule() -> None:
    client = _client()
    client.post.return_value = ok({"status": "success", "rule": _rule_payload(description=None)})
    rule = AliasingRule(client, **_rule_payload())

    updated = rule.update(description=None)

    client.post.assert_called_once_with(
        "/aliasing/4",
        json={
            "signal_name": "car.speed",
            "signal_hierarchy": ["VehSpd", "veh_speed_kph"],
            "description": None,
            "streams": [7],
            "copy_metadata": True,
        },
    )
    assert updated.description is None

    rule.update(streams=None, signal_hierarchy=["a"])
    assert client.post.call_args.kwargs["json"]["streams"] == []
    assert client.post.call_args.kwargs["json"]["signal_hierarchy"] == ["a"]


def test_delete_aliasing_rule() -> None:
    client = _client()
    client.delete.return_value = ok({"status": "success", "id": 4})

    AliasingRule(client, **_rule_payload()).delete()
    aliasing.delete_rule(client, 9)

    assert [c.args[0] for c in client.delete.call_args_list] == ["/aliasing/4", "/aliasing/9"]


def test_upsert_aliasing_rules_builds_csv() -> None:
    client = _client()
    client.post.return_value = ok([_rule_payload(), _rule_payload(id=5, signal_name="rpm")])
    stream = MagicMock()
    stream.id = 9

    rules = aliasing.upsert_rules(
        client,
        [
            {"signal_name": "car.speed", "signal_hierarchy": ["VehSpd", "kph"], "streams": [7, stream]},
            {"signal_name": "rpm", "signal_hierarchy": "EngSpd, 12", "description": "x", "copy_metadata": True},
        ],
    )

    assert client.post.call_args.args[0] == "/aliasing-upsert"
    assert _uploaded_rows(client) == [
        {
            "signal_name": "car.speed",
            "signal_hierarchy": "VehSpd,kph,",
            "description": "",
            "streams": "7,9,",
            "copy_metadata": "false",
        },
        {
            "signal_name": "rpm",
            "signal_hierarchy": "EngSpd,12,",
            "description": "x",
            "streams": "",
            "copy_metadata": "true",
        },
    ]
    assert [r.id for r in rules] == [4, 5]


def test_upsert_aliasing_rules_accepts_dataframe_file_and_rules(tmp_path) -> None:
    client = _client()
    client.post.return_value = ok([_rule_payload()])
    expected = [
        {
            "signal_name": "car.speed",
            "signal_hierarchy": "VehSpd,veh_speed_kph,",
            "description": "Vehicle speed",
            "streams": "7,",
            "copy_metadata": "true",
        }
    ]

    aliasing.upsert_rules(client, [AliasingRule(client, **_rule_payload())])
    assert _uploaded_rows(client) == expected

    frame = pd.DataFrame(
        {
            "signal_name": ["car.speed"],
            "signal_hierarchy": ["VehSpd,veh_speed_kph"],
            "description": ["Vehicle speed"],
            "streams": [7],
            "copy_metadata": [True],
        }
    )
    aliasing.upsert_rules(client, frame)
    assert _uploaded_rows(client) == expected

    path = tmp_path / "rules.csv"
    frame.to_csv(path, index=False)
    aliasing.upsert_rules(client, path)
    assert _uploaded_rows(client) == expected


def test_upsert_aliasing_rules_accepts_dataframe_with_empty_cells() -> None:
    client = _client()
    client.post.return_value = ok([])
    # A missing stream turns the column into floats; pd.NA is the empty cell of the nullable dtypes
    frame = pd.DataFrame(
        {
            "signal_name": ["car.speed", "car.rpm"],
            "signal_hierarchy": [["VehSpd"], ["EngRpm"]],
            "description": [pd.NA, "Engine speed"],
            "streams": [7, None],
            "copy_metadata": pd.array([True, pd.NA], dtype="boolean"),
        }
    )
    aliasing.upsert_rules(client, frame)
    rows = _uploaded_rows(client)
    assert [row["streams"] for row in rows] == ["7,", ""]
    assert [row["description"] for row in rows] == ["", "Engine speed"]
    assert [row["copy_metadata"] for row in rows] == ["true", "false"]


@pytest.mark.parametrize("text", ["NA", "null", "None", "1234", "1e5", "true", " "])
def test_upsert_aliasing_rules_rejects_cells_the_server_would_retype(text: str) -> None:
    client = _client()
    with pytest.raises(ValueError, match="cannot be upserted in bulk"):
        aliasing.upsert_rules(
            client, [{"signal_name": "car.speed", "signal_hierarchy": ["a"], "description": text}]
        )
    if text.strip():
        with pytest.raises(ValueError, match="cannot be upserted in bulk"):
            aliasing.upsert_rules(client, [{"signal_name": text, "signal_hierarchy": ["a"]}])
    client.post.assert_not_called()


def test_streams_must_be_a_list() -> None:
    client = _client()
    stream = MagicMock(spec=DataStream)
    with pytest.raises(TypeError, match="not a single stream"):
        aliasing.create_rule(client, "car.speed", ["VehSpd"], streams=stream)
    with pytest.raises(TypeError, match="not a single stream"):
        aliasing.create_rule(client, "car.speed", ["VehSpd"], streams="Car")


def test_upsert_aliasing_rules_rejects_bad_rows() -> None:
    client = _client()
    with pytest.raises(ValueError, match="signal_name and signal_hierarchy are required"):
        aliasing.upsert_rules(client, [{"signal_name": "car.speed"}])
    with pytest.raises(ValueError, match="comma"):
        aliasing.upsert_rules(client, [{"signal_name": "car.speed", "signal_hierarchy": ["a,b"]}])
    assert aliasing.upsert_rules(client, []) == []
    client.post.assert_not_called()


@pytest.mark.integration
def test_aliasing_e2e(db: DB) -> None:
    require_admin_api(db)
    alias = unique_name("py-sdk-alias")
    bulk_alias = unique_name("py-sdk-alias-bulk")

    with isolated_stream(db, ALIASING_TEST_PREFIX, "rules") as stream:
        rule = db.create_aliasing_rule(
            alias, ["missing.signal", "car.speed"], streams=[stream], description="e2e"
        )
        try:
            assert rule.streams == [stream.id] and rule.applies_to(stream) and not rule.copy_metadata
            with pytest.raises(ValueError, match="already exists"):
                db.create_aliasing_rule(alias, ["car.speed"])

            dataset = ingest_dataset(stream)
            aliased = dataset.get_signal(alias)
            assert aliased is not None, f"{alias} not in dataset after import"
            # From the aliased signal to its source and its rules, and back
            assert aliased.alias_of == "car.speed"
            assert [r.id for r in aliased.get_aliasing_rules()] == [rule.id]
            original = dataset.get_signal("car.speed")
            assert original is not None and original.alias_of is None
            assert [r.id for r in original.get_aliasing_rules()] == [rule.id]

            stats = poll(lambda: db.get_signal_stats(alias, stream=stream), lambda s: s.imported_count == 1)
            assert [r.id for r in stats.get_aliasing_rules()] == [rule.id]
            assert [r.id for r in db.get_aliasing_rules("car.speed", stream=stream)] == [rule.id]
            assert rule.id in [r.id for r in db.get_aliasing_rules(stream=stream)]
            assert rule.get_signal_stats(stream=stream).id == stats.id

            updated = rule.update(description="updated")
            assert updated.description == "updated"
            assert (updated.signal_hierarchy, updated.streams) == (rule.signal_hierarchy, rule.streams)

            # Bulk upsert: changes the existing rule and creates a new one, matched on signal_name
            upserted = db.upsert_aliasing_rules(
                [
                    {"signal_name": alias, "signal_hierarchy": ["car.speed"], "streams": [stream.name]},
                    {
                        "signal_name": bulk_alias,
                        "signal_hierarchy": ["car.accel", "1234"],
                        "streams": [stream],
                        "description": "bulk",
                        "copy_metadata": True,
                    },
                ]
            )
            by_name = {r.signal_name: r for r in upserted}
            assert by_name[alias].id == rule.id and by_name[alias].signal_hierarchy == ["car.speed"]
            assert by_name[alias].streams == [stream.id] and by_name[alias].description is None
            bulk = by_name[bulk_alias]
            assert bulk.signal_hierarchy == ["car.accel", "1234"] and bulk.streams == [stream.id]
            assert bulk.copy_metadata and bulk.description == "bulk"
        finally:
            for leftover in db.get_aliasing_rules():
                if leftover.signal_name in (alias, bulk_alias):
                    leftover.delete()

        assert not [r for r in db.get_aliasing_rules() if r.signal_name in (alias, bulk_alias)]
