import csv
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from marple import DB
from marple.db import SignalStats, SignalStatsDetail, signal_stats
from support import (
    ingest_dataset,
    isolated_stream,
    ok,
    poll,
    require_admin_api,
    require_signal_stats_api,
)

SIGNAL_STATS_TEST_PREFIX = "Salty Compulsory PytestSignalStats"
BASE = "/datapool/default/signal-stats"


def _stats_payload(**overrides: Any) -> dict[str, Any]:
    payload = {
        "id": 5,
        "name": "car.speed",
        "n_streams": 2,
        "imported_count": 124,
        "coverage": 0.5,
        "imported_first": "2026-10-01T00:00:00Z",
        "imported_last": "2026-11-01T00:00:00Z",
        "time_min": 0,
        "time_max": 10,
        "duration_total": 10,
        "count": 1000,
        "count_value": 1000,
        "count_text": 0,
        "value_min": 0.0,
        "value_max": 80.0,
        "value_mean": 40.0,
        "value_min_dataset": {"id": 1, "stream_id": 7, "path": "a.csv", "created_at": "2026-10-01T00:00:00Z"},
        "value_max_dataset": None,
        "frequency_min": 10.0,
        "frequency_max": 100.0,
        "units": {"km/h": 124},
        "original_names": {"VehSpd": 120, "veh_speed_kph": 4, "car.speed": 1},
        "cold_bytes": 10,
        "cold_file_count": 1,
        "hot_bytes": 0,
        "n_hot": 0,
        "n_archived": 0,
        "archive_bytes": 0,
        "heated_counter": 0,
        "heated_last": None,
        "priority_stream_ids": [],
    }
    payload.update(overrides)
    return payload


def _client() -> MagicMock:
    client = MagicMock()
    client.datapool = "default"
    return client


def _page(start: int, n: int, total: int) -> Any:
    signals = [_stats_payload(id=i, name=f"s{i}") for i in range(start, start + n)]
    return ok({"total": total, "signals": signals, "collected_at": "2026-11-18T10:00:00"})


def test_search_signals_sends_query_parameters() -> None:
    client = _client()
    client.get.return_value = _page(0, 2, 2)

    result = signal_stats.search(
        client, "car.* unit:rpm", stream=7, sort="cold_bytes", descending=False, limit=10
    )

    client.get.assert_called_once_with(
        BASE,
        params={
            "sort": "cold_bytes",
            "descending": False,
            "stream_id": 7,
            "q": "car.* unit:rpm",
            "limit": 10,
            "offset": 0,
        },
    )
    assert [s.name for s in result] == ["s0", "s1"]
    assert result.total == 2 and result.collected_at is not None
    assert result[0].stream_id == 7
    assert list(result.to_dataframe()["priority"]) == ["default", "default"]


def test_search_signals_without_limit_follows_total() -> None:
    client = _client()
    client.get.side_effect = [_page(0, 1000, 2500), _page(1000, 1000, 2500), _page(2000, 500, 2500)]

    result = signal_stats.search(client, limit=None)

    assert len(result) == 2500 and result.total == 2500
    calls = [(c.kwargs["params"]["limit"], c.kwargs["params"]["offset"]) for c in client.get.call_args_list]
    assert calls == [(1000, 0), (1000, 1000), (1000, 2000)]
    assert "stream_id" not in client.get.call_args.kwargs["params"]


def test_search_signals_limit_spans_pages_and_stops() -> None:
    client = _client()
    client.get.side_effect = [_page(0, 1000, 5000), _page(1000, 200, 5000)]

    result = signal_stats.search(client, limit=1200)

    assert len(result) == 1200 and result.total == 5000
    assert client.get.call_args_list[1].kwargs["params"]["limit"] == 200
    # A slice is still a result of the same search
    assert len(result[:10]) == 10 and result[:10].total == 5000
    assert result[:10].collected_at == result.collected_at


def test_search_signals_errors() -> None:
    client = _client()
    with pytest.raises(ValueError, match="Cannot sort signals on 'nope'"):
        signal_stats.search(client, sort="nope")
    client.get.assert_not_called()

    client.get.return_value = ok({"error": "`frequency>=` needs a value"}, 400)
    with pytest.raises(ValueError, match="needs a value"):
        signal_stats.search(client, "frequency>=")

    client.get.return_value = ok({"error": "still being collected"}, 503)
    with pytest.raises(RuntimeError, match="still being collected"):
        signal_stats.search(client)


def test_get_signal_stats_resolves_names() -> None:
    client = _client()
    client.get_signal_id.return_value = 5
    client.get.return_value = ok(_stats_payload(streams=[]))

    detail = signal_stats.get(client, "car.speed", stream=7)

    client.get_signal_id.assert_called_once_with("car.speed")
    client.get.assert_called_once_with(f"{BASE}/5", params={"stream_id": 7})
    assert isinstance(detail, SignalStatsDetail) and detail.stream_id == 7

    client.get_signal_id.return_value = None
    with pytest.raises(ValueError, match="'missing' not found"):
        signal_stats.get(client, "missing")

    client.get.return_value = ok({"detail": "Signal not found"}, 404)
    with pytest.raises(ValueError, match="No signal stats for signal 99"):
        signal_stats.get(client, 99)


def test_priority_property() -> None:
    client = _client()
    assert SignalStats(client, **_stats_payload()).priority == "default"
    assert SignalStats(client, **_stats_payload(priority_stream_ids=[7])).priority == "partial"
    assert SignalStats(client, **_stats_payload(priority_stream_ids=[7, 8])).priority == "high"


def test_get_datasets_follows_next() -> None:
    client = _client()
    stats = SignalStats(client, 7, **_stats_payload())

    def occurrence(dataset_id: int) -> dict[str, Any]:
        return {
            "dataset": {"id": dataset_id, "stream_id": 7, "path": "a.csv", "created_at": "2026-10-01T00:00:00Z"},
            "value_min": 0.0,
            "value_max": 1.0,
            "value_mean": 0.5,
            "count": 10,
            "duration": 10,
            "frequency": 1.0,
            "unit": None,
            "original_name": "VehSpd",
            "storage_status": "ARCHIVE",
            "heated_counter": 0,
        }

    client.get.side_effect = [
        ok({"datasets": [occurrence(i) for i in range(1000, 0, -1)], "next": 1}),
        ok({"datasets": [occurrence(0)], "next": None}),
    ]
    occurrences = stats.get_datasets(limit=None)

    assert len(occurrences) == 1001 and occurrences[0].storage_status == "ARCHIVE"
    first, second = (c.kwargs["params"] for c in client.get.call_args_list)
    assert first == {"stream_id": 7, "limit": 1000}
    assert second == {"stream_id": 7, "limit": 1000, "before": 1}
    assert client.get.call_args.args[0] == f"{BASE}/5/datasets"

    # An explicit stream=None widens a stream-scoped object to the pool; limit caps the page
    client.get.side_effect = [ok({"datasets": [occurrence(3)], "next": 2})]
    assert len(stats.get_datasets(stream=None, limit=1)) == 1
    assert client.get.call_args.kwargs["params"] == {"limit": 1}


def test_set_signal_priority_batches_and_sums() -> None:
    client = _client()
    client.get_signal_id.return_value = 3
    client.post.side_effect = lambda url, params, json: ok({"signals": len(json["signal_ids"]), "rules": 1})
    signals = [*range(25_000), "car.speed", SignalStats(client, **_stats_payload(id=77))]

    result = signal_stats.set_priority(client, signals, stream=7)

    assert (result.signals, result.rules) == (25_002, 3)
    batches = [c.kwargs["json"] for c in client.post.call_args_list]
    assert [len(b["signal_ids"]) for b in batches] == [10_000, 10_000, 5_002]
    assert batches[-1]["signal_ids"][-2:] == [3, 77]
    assert all(b["priority"] == "high" for b in batches)
    assert client.post.call_args.args[0] == f"{BASE}/priority"
    assert client.post.call_args.kwargs["params"] == {"stream_id": 7}


def test_set_priority_on_stats_keeps_scope() -> None:
    client = _client()
    client.post.return_value = ok({"signals": 1, "rules": 1})

    SignalStats(client, 7, **_stats_payload()).set_priority(False)
    assert client.post.call_args.kwargs == {
        "params": {"stream_id": 7},
        "json": {"signal_ids": [5], "priority": "default"},
    }

    client.post.reset_mock()
    assert signal_stats.set_priority(client, []).signals == 0
    client.post.assert_not_called()


def test_import_signal_priority_uploads_file(tmp_path: Path) -> None:
    client = _client()
    client.post.return_value = ok({"signals": 2, "unknown": 1, "skipped": 0, "rules": 1})
    path = tmp_path / "signals.csv"
    path.write_text("name,priority\ncar.speed,high\n")

    result = signal_stats.import_priority(client, path, stream=7)

    assert result.unknown == 1
    assert client.post.call_args.args[0] == f"{BASE}/priority/import"
    assert client.post.call_args.kwargs["params"] == {"stream_id": 7}
    name, _, content_type = client.post.call_args.kwargs["files"]["file"]
    assert (name, content_type) == ("signals.csv", "text/csv")


def test_export_signal_stats_streams_to_disk(tmp_path: Path) -> None:
    response = MagicMock(status_code=200)
    response.__enter__.return_value = response
    response.iter_content.return_value = [b"priority,id,name\n", b",5,car.speed\n"]
    client = _client()
    client.get.return_value = response

    path = signal_stats.export(client, tmp_path, "car.*", stream=7)

    assert path == tmp_path / "signals.csv"
    assert path.read_text() == "priority,id,name\n,5,car.speed\n"
    assert client.get.call_args.args[0] == f"{BASE}/export"
    assert client.get.call_args.kwargs["params"]["q"] == "car.*"
    assert client.get.call_args.kwargs["stream"] is True


@pytest.mark.integration
def test_signal_stats_e2e(db: DB, tmp_path: Path) -> None:
    require_signal_stats_api(db)

    with isolated_stream(db, SIGNAL_STATS_TEST_PREFIX, "read") as stream:
        dataset = ingest_dataset(stream)
        found = poll(lambda: stream.search_signals("car.speed"), lambda r: any(s.name == "car.speed" for s in r))
        speed = next(s for s in found if s.name == "car.speed")
        assert speed.stream_id == stream.id and speed.imported_count == 1

        filtered = db.search_signals("car.*", stream=stream, sort="name", descending=False, limit=None)
        assert filtered.total == len(filtered) > 1
        assert all(s.name.startswith("car.") for s in filtered)
        assert [s.name for s in filtered] == sorted(s.name for s in filtered)
        assert len(db.search_signals("car.*", stream=stream.name, limit=2)) == 2
        with pytest.raises(ValueError, match="needs a value"):
            db.search_signals("frequency>=", stream=stream)

        detail = db.get_signal_stats("car.speed", stream=stream)
        assert detail.id == speed.id and [s.stream_id for s in detail.streams] == [stream.id]
        assert speed.refresh().count == detail.count > 0
        occurrences = speed.get_datasets()
        assert [o.dataset.id for o in occurrences] == [dataset.id]

        summary = db.get_signal_stats_summary(stream=stream)
        assert summary.n_datasets == 1 and summary.n_signals == len(stream.search_signals(limit=None))
        assert db.get_signal_stats_summary("car.speed", stream=stream).n_signals >= 1

        path = db.export_signal_stats(tmp_path, "car.*", stream=stream)
        with open(path) as f:
            rows = list(csv.DictReader(f))
        assert {row["name"] for row in rows} == {s.name for s in filtered}


@pytest.mark.integration
def test_signal_priority_e2e(db: DB, tmp_path: Path) -> None:
    require_admin_api(db)

    def stats() -> SignalStatsDetail:
        return db.get_signal_stats("car.speed", stream=stream)

    with isolated_stream(db, SIGNAL_STATS_TEST_PREFIX, "priority") as stream:
        ingest_dataset(stream)
        speed = poll(stats, lambda s: s.imported_count == 1)
        assert speed.priority == "default"

        result = db.set_signal_priority("car.speed", stream=stream)
        assert (result.signals, result.rules) == (1, 1)
        assert stats().priority_stream_ids == [stream.id] and stats().priority == "high"
        assert "car.speed" in [s.name for s in stream.search_signals("is:priority")]

        assert speed.set_priority(False).rules == 1
        assert stats().priority == "default"

        # Round trip: export, edit the priority column, import
        path = db.export_signal_stats(tmp_path / "signals.csv", "car.speed", stream=stream)
        with open(path) as f:
            rows = list(csv.DictReader(f))
        assert rows and all(row["priority"] == "" for row in rows)
        for row in rows:
            row["priority"] = "high" if row["name"] == "car.speed" else row["priority"]
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)

        imported = db.import_signal_priority(path, stream=stream)
        assert imported.signals == len(rows) and imported.unknown == 0 and imported.rules == 1
        assert stats().priority == "high"
