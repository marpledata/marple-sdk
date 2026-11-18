"""Metadata fields: the fields a workspace defines for the metadata of its datasets."""

from collections.abc import Iterable
from typing import Any, Union

from pydantic import BaseModel, Field, PrivateAttr, field_validator

from marple.db.activity import logger
from marple.utils import (
    OMITTED,
    DBClient,
    Omitted,
    StreamKey,
    resolve_stream_ids,
    validate_response,
)


class MetadataField(BaseModel):
    """
    A metadata field defined for the workspace.

    This is the definition of a field (its name, allowed values, whether it is mandatory), not
    the metadata of one dataset: that is :attr:`Dataset.metadata <marple.db.Dataset.metadata>`.
    """

    id: int
    name: str
    description: str | None = None
    allowed_values: list[str] = Field(default_factory=list)
    """The values the field may take. Empty means any value."""
    streams: list[int] = Field(default_factory=list)
    """Stream ids this field belongs to. Empty means every stream."""
    mandatory: bool = False
    """Whether a dataset must have a value for this field."""
    user_input: bool = False
    """Whether users are asked for this field when they upload a file."""

    _client: DBClient = PrivateAttr()

    def __init__(self, client: DBClient, **kwargs):
        super().__init__(**kwargs)
        self._client = client

    @field_validator("allowed_values", "streams", mode="before")
    @classmethod
    def _default_list(cls, value: object) -> object:
        return value or []

    def update(
        self,
        *,
        name: str | Omitted = OMITTED,
        description: str | None | Omitted = OMITTED,
        allowed_values: Iterable[str] | None | Omitted = OMITTED,
        streams: Iterable[StreamKey] | None | Omitted = OMITTED,
        mandatory: bool | Omitted = OMITTED,
        user_input: bool | Omitted = OMITTED,
    ) -> "MetadataField":
        """
        Update this field and return the updated field. Fields that are not given keep their value.

        Requires an admin token.

        Args:
            name: The name of the field.
            description: Free text.
            allowed_values: The values the field may take. ``None`` or empty allows any value.
            streams: The streams the field belongs to (ids, names or ``DataStream`` objects).
                ``None`` or empty means every stream.
            mandatory: Whether a dataset must have a value for this field.
            user_input: Whether users are asked for this field when they upload a file.
        """
        # The server replaces the whole field and resets what is left out, so send every field
        payload: dict[str, Any] = self.model_dump(exclude={"id"})
        if not isinstance(name, Omitted):
            payload["name"] = name
        if not isinstance(description, Omitted):
            payload["description"] = description
        if not isinstance(allowed_values, Omitted):
            payload["allowed_values"] = _values(allowed_values)
        if not isinstance(streams, Omitted):
            payload["streams"] = resolve_stream_ids(self._client, streams)
        if not isinstance(mandatory, Omitted):
            payload["mandatory"] = mandatory
        if not isinstance(user_input, Omitted):
            payload["user_input"] = user_input

        r = self._client.post(f"/metadata/{self.id}", json=payload)
        updated = MetadataField(self._client, **validate_response(r, "Update metadata field failed"))
        logger.debug(f"Updated metadata field {updated.name}")
        return updated

    def delete(self) -> None:
        """
        Delete this field definition. Requires an admin token.

        Metadata values that datasets already have are kept.
        """
        delete_field(self._client, self)


MetadataFieldKey = Union[int, str, MetadataField]
"""How a metadata field can be given: its id, its name, or a :class:`MetadataField`."""


def list_fields(client: DBClient) -> list[MetadataField]:
    """See :meth:`marple.db.DB.get_metadata_fields`."""
    r = client.get("/metadata")
    return [MetadataField(client, **field) for field in validate_response(r, "Get metadata fields failed")]


def get_field(client: DBClient, key: MetadataFieldKey) -> MetadataField:
    """See :meth:`marple.db.DB.get_metadata_field`."""
    fields = list_fields(client)
    wanted_id = key.id if isinstance(key, MetadataField) else key
    for field in fields:
        if field.id == wanted_id or (isinstance(key, str) and field.name == key):
            return field
    raise ValueError(
        f"Metadata field {key!r} not found, available fields: {', '.join(field.name for field in fields)}"
    )


def create_field(
    client: DBClient,
    name: str,
    *,
    description: str | None = None,
    allowed_values: Iterable[str] | None = None,
    streams: Iterable[StreamKey] | None = None,
    mandatory: bool = False,
    user_input: bool = False,
) -> MetadataField:
    """See :meth:`marple.db.DB.create_metadata_field`."""
    r = client.post(
        "/metadata",
        json={
            "name": name,
            "description": description,
            "allowed_values": _values(allowed_values),
            "streams": resolve_stream_ids(client, streams),
            "mandatory": mandatory,
            "user_input": user_input,
        },
    )
    field = MetadataField(client, **validate_response(r, "Create metadata field failed"))
    logger.debug(f"Created metadata field {name}")
    return field


def delete_field(client: DBClient, key: MetadataFieldKey) -> None:
    """See :meth:`marple.db.DB.delete_metadata_field`."""
    field_id = key.id if isinstance(key, MetadataField) else key
    if isinstance(field_id, str):
        field_id = get_field(client, field_id).id
    r = client.delete(f"/metadata/{field_id}")
    validate_response(r, "Delete metadata field failed")
    logger.debug(f"Deleted metadata field {field_id}")


def _values(allowed_values: "Iterable[str] | None") -> list[str]:
    if isinstance(allowed_values, str):
        raise TypeError("allowed_values must be a list of values, not a single string")
    return [str(value) for value in allowed_values or []]
