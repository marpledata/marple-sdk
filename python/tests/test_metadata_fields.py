from typing import Any
from unittest.mock import MagicMock

import pytest

from marple import DB
from marple.db import MetadataField
from marple.db import metadata as metadata_fields
from support import isolated_stream, ok, require_admin_api, unique_name

METADATA_TEST_PREFIX = "Salty Compulsory PytestMetadataFields"


def _field_payload(**overrides: Any) -> dict[str, Any]:
    payload = {
        "id": 3,
        "name": "track",
        "description": "Race track",
        "allowed_values": ["Spa", "Zolder"],
        "streams": [7],
        "mandatory": True,
        "user_input": True,
    }
    payload.update(overrides)
    return payload


def test_get_metadata_field_by_id_or_name() -> None:
    client = MagicMock()
    client.get.return_value = ok([_field_payload(), _field_payload(id=4, name="driver", streams=None)])

    assert [f.name for f in metadata_fields.list_fields(client)] == ["track", "driver"]
    assert metadata_fields.get_field(client, 4).name == "driver"
    assert metadata_fields.get_field(client, "track").id == 3
    assert metadata_fields.get_field(client, 4).streams == []
    with pytest.raises(ValueError, match="'car' not found, available fields: track, driver"):
        metadata_fields.get_field(client, "car")
    client.get.assert_called_with("/metadata")


def test_create_metadata_field() -> None:
    client = MagicMock()
    client.post.return_value = ok(_field_payload())

    field = metadata_fields.create_field(client, "track", allowed_values=["Spa", "Zolder"], streams=[7])

    client.post.assert_called_once_with(
        "/metadata",
        json={
            "name": "track",
            "description": None,
            "allowed_values": ["Spa", "Zolder"],
            "streams": [7],
            "mandatory": False,
            "user_input": False,
        },
    )
    assert field.id == 3
    with pytest.raises(TypeError, match="not a single string"):
        metadata_fields.create_field(client, "track", allowed_values="Spa")  # type: ignore[arg-type]


def test_update_sends_merged_full_field() -> None:
    client = MagicMock()
    client.post.return_value = ok(_field_payload(allowed_values=["Monza"]))
    field = MetadataField(client, **_field_payload())

    updated = field.update(allowed_values=["Monza"])

    # The server resets every field left out, so the unchanged ones must be in the body
    client.post.assert_called_once_with(
        "/metadata/3",
        json={
            "name": "track",
            "description": "Race track",
            "allowed_values": ["Monza"],
            "streams": [7],
            "mandatory": True,
            "user_input": True,
        },
    )
    assert updated.allowed_values == ["Monza"]

    field.update(description=None, streams=None, mandatory=False)
    body = client.post.call_args.kwargs["json"]
    assert (body["description"], body["streams"], body["mandatory"]) == (None, [], False)
    assert body["allowed_values"] == ["Spa", "Zolder"] and body["user_input"] is True


def test_delete_metadata_field_by_id_name_or_object() -> None:
    client = MagicMock()
    client.get.return_value = ok([_field_payload()])
    client.delete.return_value = ok(3)

    metadata_fields.delete_field(client, 9)
    metadata_fields.delete_field(client, "track")
    MetadataField(client, **_field_payload(id=5)).delete()

    assert [c.args[0] for c in client.delete.call_args_list] == ["/metadata/9", "/metadata/3", "/metadata/5"]


@pytest.mark.integration
def test_metadata_fields_e2e(db: DB) -> None:
    require_admin_api(db)
    name = unique_name("py-sdk-field")

    with isolated_stream(db, METADATA_TEST_PREFIX, "fields") as stream:
        field = db.create_metadata_field(
            name, description="e2e", allowed_values=["a", "b"], streams=[stream], mandatory=True
        )
        try:
            assert (field.name, field.streams, field.mandatory, field.user_input) == (
                name,
                [stream.id],
                True,
                False,
            )
            assert db.get_metadata_field(name).id == field.id
            assert db.get_metadata_field(field.id).name == name
            assert field.id in [f.id for f in db.get_metadata_fields()]
            with pytest.raises(ValueError, match="already exists"):
                db.create_metadata_field(name)

            updated = field.update(allowed_values=["a", "b", "c"])
            assert updated.allowed_values == ["a", "b", "c"]
            assert (updated.name, updated.description, updated.streams) == (name, "e2e", [stream.id])
            assert updated.mandatory is True and updated.user_input is False
            assert db.get_metadata_field(field.id) == updated

            cleared = updated.update(streams=None, mandatory=False)
            assert cleared.streams == [] and cleared.mandatory is False
            assert cleared.allowed_values == ["a", "b", "c"]
        finally:
            db.delete_metadata_field(field)

        with pytest.raises(ValueError, match="not found"):
            db.get_metadata_field(name)
