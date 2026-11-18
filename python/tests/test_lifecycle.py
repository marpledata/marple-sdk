from typing import Any
from unittest.mock import MagicMock

import pytest

from marple import DB
from marple.db import LifecycleRules, lifecycle
from support import isolated_stream, ok, require_admin_api

LIFECYCLE_TEST_PREFIX = "Salty Compulsory PytestLifecycle"

WORKSPACE = {
    "stream_id": None,
    "hot_to_cold_days": 30,
    "cold_to_archive_days": 0,
    "realtime_to_cold_days": 7,
    "retain_prio_signals": False,
}
STREAM_7 = {
    "stream_id": 7,
    "hot_to_cold_days": 60,
    "cold_to_archive_days": None,
    "realtime_to_cold_days": None,
    "retain_prio_signals": True,
}


def _client() -> MagicMock:
    client = MagicMock()
    client.get.return_value = ok([WORKSPACE, STREAM_7])
    client.post.side_effect = lambda url, json: ok(json)
    return client


def _posted(client: MagicMock) -> dict[str, Any]:
    client.post.assert_called_once()
    assert client.post.call_args.args[0] == "/lifecycle-rules"
    return client.post.call_args.kwargs["json"]


def test_get_lifecycle_rules() -> None:
    client = _client()

    assert [r.stream_id for r in lifecycle.list_rules(client)] == [None, 7]
    assert lifecycle.get_rules(client).model_dump() == WORKSPACE
    assert lifecycle.get_rules(client, 7).model_dump() == STREAM_7
    # A stream without rules of its own inherits everything
    assert lifecycle.get_rules(client, 8) == LifecycleRules(stream_id=8)
    client.get.assert_called_with("/lifecycle-rules")


def test_get_effective_lifecycle_rules() -> None:
    client = _client()

    assert lifecycle.get_rules(client, 7, effective=True).model_dump() == {
        "stream_id": 7,
        "hot_to_cold_days": 60,
        "cold_to_archive_days": 0,
        "realtime_to_cold_days": 7,
        "retain_prio_signals": True,
    }
    assert lifecycle.get_rules(client, 8, effective=True).model_dump() == {**WORKSPACE, "stream_id": 8}


def test_set_lifecycle_rules_sends_merged_full_body() -> None:
    client = _client()
    rules = lifecycle.set_rules(client, 7, cold_to_archive_days=90, hot_to_cold_days=None)

    assert _posted(client) == {
        "stream_id": 7,
        "hot_to_cold_days": None,
        "cold_to_archive_days": 90,
        "realtime_to_cold_days": None,
        "retain_prio_signals": True,
    }
    assert rules.cold_to_archive_days == 90

    client = _client()
    lifecycle.set_rules(client, 8, hot_to_cold_days=45)
    assert _posted(client) == {**LifecycleRules(stream_id=8).model_dump(), "hot_to_cold_days": 45}

    client = _client()
    lifecycle.set_rules(client, retain_prio_signals=True)
    assert _posted(client) == {**WORKSPACE, "retain_prio_signals": True}


def test_set_workspace_lifecycle_rules_rejects_none() -> None:
    client = _client()
    with pytest.raises(ValueError, match="cannot be None \\(hot_to_cold_days\\)"):
        lifecycle.set_rules(client, hot_to_cold_days=None)
    client.post.assert_not_called()


@pytest.mark.integration
def test_lifecycle_rules_e2e(db: DB) -> None:
    require_admin_api(db)

    workspace = db.get_lifecycle_rules()
    assert workspace.stream_id is None
    assert None not in workspace.model_dump(exclude={"stream_id"}).values()
    assert db.list_lifecycle_rules()[0] == workspace

    with isolated_stream(db, LIFECYCLE_TEST_PREFIX, "rules") as stream:
        inherited = LifecycleRules(stream_id=stream.id)
        assert stream.get_lifecycle_rules() == inherited
        assert stream.get_lifecycle_rules(effective=True) == workspace.model_copy(
            update={"stream_id": stream.id}
        )

        rules = db.set_lifecycle_rules(stream, hot_to_cold_days=45)
        assert rules == inherited.model_copy(update={"hot_to_cold_days": 45})
        assert db.get_lifecycle_rules(stream) == rules
        assert rules in db.list_lifecycle_rules()

        # A second partial update keeps the first
        rules = stream.set_lifecycle_rules(cold_to_archive_days=0)
        assert (rules.hot_to_cold_days, rules.cold_to_archive_days) == (45, 0)
        effective = stream.get_lifecycle_rules(effective=True)
        assert (effective.hot_to_cold_days, effective.cold_to_archive_days) == (45, 0)
        assert effective.realtime_to_cold_days == workspace.realtime_to_cold_days

        with pytest.raises(ValueError, match="hot_to_cold_days must be"):
            stream.set_lifecycle_rules(hot_to_cold_days=7)

        assert stream.set_lifecycle_rules(hot_to_cold_days=None, cold_to_archive_days=None) == inherited


@pytest.mark.integration
def test_workspace_lifecycle_rules_e2e(db: DB) -> None:
    require_admin_api(db)
    original = db.get_lifecycle_rules()
    changed_days = 31 if original.hot_to_cold_days != 31 else 32
    try:
        db.set_lifecycle_rules()  # writes the rules as they are
    except ValueError as e:
        pytest.skip(f"The stored workspace rules are not valid on this server, so they cannot be restored: {e}")

    try:
        changed = db.set_lifecycle_rules(hot_to_cold_days=changed_days)
        assert changed == original.model_copy(update={"hot_to_cold_days": changed_days})
        assert db.get_lifecycle_rules() == changed
    finally:
        restored = db.set_lifecycle_rules(**original.model_dump(exclude={"stream_id"}))
    assert restored == original
