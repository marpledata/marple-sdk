"""Lifecycle rules: when data moves from hot to cold storage and from cold storage to the archive."""

from typing import Any

from pydantic import BaseModel

from marple.db.activity import logger
from marple.utils import (
    OMITTED,
    DBClient,
    Omitted,
    StreamKey,
    resolve_stream_id,
    validate_response,
)

RULES = ("hot_to_cold_days", "cold_to_archive_days", "realtime_to_cold_days", "retain_prio_signals")


class LifecycleRules(BaseModel):
    """
    The lifecycle rules of the workspace (``stream_id`` is ``None``) or of one stream.

    The ``*_days`` fields are the number of days before data moves to the next storage tier.
    ``0`` means never. On a stream, ``None`` means the stream inherits the workspace value.
    """

    stream_id: int | None = None
    """The stream these rules belong to, or ``None`` for the workspace rules."""
    hot_to_cold_days: int | None = None
    """Days without use before a signal leaves hot storage."""
    cold_to_archive_days: int | None = None
    """Days before cold data is archived: only the original file is kept. Not for realtime streams."""
    realtime_to_cold_days: int | None = None
    """Days before a realtime dataset is moved to cold storage."""
    retain_prio_signals: bool | None = None
    """Whether signals with high import priority are kept out of the archive. Not for realtime streams."""

    def resolve(self, workspace: "LifecycleRules") -> "LifecycleRules":
        """These rules with every inherited (``None``) value filled in from the workspace rules."""
        own = {rule: value for rule in RULES if (value := getattr(self, rule)) is not None}
        return workspace.model_copy(update={"stream_id": self.stream_id, **own})


def list_rules(client: DBClient) -> list[LifecycleRules]:
    """See :meth:`marple.db.DB.list_lifecycle_rules`."""
    r = client.get("/lifecycle-rules")
    return [LifecycleRules.model_validate(rules) for rules in validate_response(r, "Get lifecycle rules failed")]


def get_rules(client: DBClient, stream: StreamKey | None = None, *, effective: bool = False) -> LifecycleRules:
    """See :meth:`marple.db.DB.get_lifecycle_rules`."""
    stream_id = resolve_stream_id(client, stream)
    workspace, *streams = list_rules(client)
    if stream_id is None:
        return workspace
    own = next((rules for rules in streams if rules.stream_id == stream_id), LifecycleRules(stream_id=stream_id))
    return own.resolve(workspace) if effective else own


def set_rules(
    client: DBClient,
    stream: StreamKey | None = None,
    *,
    hot_to_cold_days: int | None | Omitted = OMITTED,
    cold_to_archive_days: int | None | Omitted = OMITTED,
    realtime_to_cold_days: int | None | Omitted = OMITTED,
    retain_prio_signals: bool | None | Omitted = OMITTED,
) -> LifecycleRules:
    """See :meth:`marple.db.DB.set_lifecycle_rules`."""
    changes = {
        "hot_to_cold_days": hot_to_cold_days,
        "cold_to_archive_days": cold_to_archive_days,
        "realtime_to_cold_days": realtime_to_cold_days,
        "retain_prio_signals": retain_prio_signals,
    }
    # The server writes the whole row, so start from the current rules and send every field
    payload: dict[str, Any] = get_rules(client, stream).model_dump()
    payload.update({rule: value for rule, value in changes.items() if not isinstance(value, Omitted)})
    if payload["stream_id"] is None:
        unset = [rule for rule in RULES if payload[rule] is None]
        if unset:
            raise ValueError(
                f"Workspace lifecycle rules cannot be None ({', '.join(unset)}): there is nothing to inherit from. "
                "Use 0 for never."
            )

    r = client.post("/lifecycle-rules", json=payload)
    rules = LifecycleRules.model_validate(validate_response(r, "Set lifecycle rules failed"))
    scope = "the workspace" if rules.stream_id is None else f"stream {rules.stream_id}"
    logger.debug(f"Set lifecycle rules of {scope}: {rules.model_dump(exclude={'stream_id'})}")
    return rules
