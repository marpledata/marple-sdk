import warnings
from collections.abc import Iterable
from functools import wraps
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Optional, Sequence, TextIO

import pandas as pd
from pydantic import ValidationError
from requests import Response
from requests.exceptions import ConnectionError

from marple.db import activity, aliasing, lifecycle
from marple.db import metadata as metadata_fields
from marple.db import signal_stats, sql
from marple.db.activity import logger
from marple.db.aliasing import AliasingRule
from marple.db.constants import LAKE_ARROW_SCHEMA as _LAKE_ARROW_SCHEMA
from marple.db.constants import SAAS_URL
from marple.db.constants import SCHEMA as _SCHEMA
from marple.db.dataset import Dataset, DatasetList
from marple.db.datastream import DataStream
from marple.db.lifecycle import LifecycleRules
from marple.db.metadata import MetadataField
from marple.db.script import SandboxJob, SandboxJobStatus, Script, ScriptVersion
from marple.db.signal import Signal, StorageStatus
from marple.db.signal_stats import (
    DatasetRef,
    HeatBucket,
    SignalOccurrence,
    SignalPriorityImportResult,
    SignalPriorityResult,
    SignalSort,
    SignalStats,
    SignalStatsDetail,
    SignalStatsList,
    SignalStatsSummary,
    SignalStreamStats,
)
from marple.db.signal_upload import SignalsAlreadyExistError, SignalUpload
from marple.utils import OMITTED, DBClient, Omitted, StreamKey, validate_response

if TYPE_CHECKING:
    from trino.dbapi import Connection

SCHEMA = _SCHEMA
"""Arrow schema for :meth:`~marple.db.Dataset.append` (long-format realtime rows)."""

LAKE_ARROW_SCHEMA = _LAKE_ARROW_SCHEMA
"""Arrow schema for :meth:`~marple.db.Dataset.add_signal` / :meth:`~marple.db.Dataset.add_signals`.

Columns: ``time`` (int64 nanoseconds, required) plus ``value`` (float64) and/or
``value_text`` (string). At least one value column is required; the other may be
omitted and is filled with nulls during validation.
"""

__all__ = [
    "DB",
    "AliasingRule",
    "DataStream",
    "Dataset",
    "DatasetList",
    "DatasetRef",
    "HeatBucket",
    "LifecycleRules",
    "MetadataField",
    "SandboxJob",
    "SandboxJobStatus",
    "Script",
    "ScriptVersion",
    "Signal",
    "SignalOccurrence",
    "SignalPriorityImportResult",
    "SignalPriorityResult",
    "SignalSort",
    "SignalStats",
    "SignalStatsDetail",
    "SignalStatsList",
    "SignalStatsSummary",
    "SignalStreamStats",
    "SignalUpload",
    "SignalsAlreadyExistError",
    "StorageStatus",
    "SCHEMA",
    "LAKE_ARROW_SCHEMA",
]


def deprecated(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        warnings.warn(
            f"The function db.{func.__name__} is deprecated and it is encouraged to use the Datastream, Dataset and Signal classes directly.",
            category=DeprecationWarning,
            stacklevel=2,
        )
        return func(*args, **kwargs)

    return wrapper


class DB:
    """
    The DB class is the main entry point for the Marple DB API.
    It provides a high-level interface for interacting with the Marple DB API.

    Args:
        api_token: The API token for the Marple DB API.
        api_url: The URL of the Marple DB API.
        datapool: The datapool to use (default: "default").
        cache_folder: The folder to cache the data in (default: "./.mdb_cache").
        trino_host: Override for the Trino query host. By default it is derived
            from `api_url` as `query.<api-host>`.
    """

    _streams: dict[int, DataStream] = {}
    client: DBClient

    def __init__(
        self,
        api_token: str,
        api_url: str = SAAS_URL,
        datapool="default",
        cache_folder: str = "./.mdb_cache",
        trino_host: str | None = None,
    ):
        self._streams: dict[int, DataStream] = {}
        self.client = DBClient(api_token, api_url, datapool, cache_folder, trino_host)
        self.check_connection()

    # Utility functions #

    def get(self, url: str, *args, **kwargs) -> Response:
        """
        Send a GET request to the Marple DB API.
        """
        return self.client.get(url, *args, **kwargs)

    def post(self, url: str, *args, **kwargs) -> Response:
        """Send a POST request to the Marple DB API."""
        return self.client.post(url, *args, **kwargs)

    def patch(self, url: str, *args, **kwargs) -> Response:
        """Send a PATCH request to the Marple DB API."""
        return self.client.patch(url, *args, **kwargs)

    def delete(self, url: str, *args, **kwargs) -> Response:
        """
        Send a DELETE request to the Marple DB API.
        """
        return self.client.delete(url, *args, **kwargs)

    def check_connection(self) -> bool:
        """
        Check if the connection to the Marple DB API is working.
         - If the connection is successful, returns True.
         - If the connection fails, raise an error.
        """
        try:
            r = self.client.get("/health")
        except ConnectionError:
            error_text = f"Could not connect to Marple DB at {self.client.api_url}. Please check if the api_url parameter is correct (ends with /api/v1) and try again."
            raise Exception(error_text)
        if r.status_code == 404:
            error_text = f"Could not find Marple DB at {r.request.url}. Please check if the api_url parameter is correct and try again."
            if not self.client.api_url.endswith("/api/v1"):
                error_text += " The api_url parameter should end with /api/v1"
            raise Exception(error_text)
        if r.status_code != 200:
            error_text = f"Unknown error occurred while connecting to Marple DB at {r.request.url}. Status code: {r.status_code}."
            raise Exception(error_text)
        try:
            status = r.json()["status"]
        except Exception:
            error_text = f"Could not connect to Marple DB at {self.client.api_url}. Please check if the api_url parameter is correct (ends with /api/v1) and try again."
            raise Exception(error_text)
        if status != "healthy":
            error_text = f"Could not connect to Marple DB at {self.client.api_url}. Please check if the api_url parameter is correct (ends with /api/v1) and try again."
            raise Exception(error_text)

        r = self.client.get("/streams")
        if r.status_code == 403:
            try:
                detail = r.json().get("detail")
            except Exception:
                detail = None
            raise Exception(
                detail
                or "Invalid API token. Please check if the api_token parameter is correct and not expired."
            )

        self._refresh_stream_cache(r)
        return True

    def verbose(self, enabled: bool = True, file: TextIO | None = None) -> None:
        """Turn SDK mutation activity logging on or off (stdout by default)."""
        activity.verbose(enabled, file)

    # Stream functions #

    def create_stream(
        self,
        name: str,
        description: Optional[str] = None,
        type: Literal["files", "realtime"] = "files",
        layer_shifts: Optional[list[int]] = None,
        datapool: Optional[str] = None,
        plugin: Optional[str] = None,
        plugin_args: Optional[str] = None,
        signal_reduction: Optional[list] = None,
        insight_workspace: Optional[str] = None,
        insight_project: Optional[str] = None,
    ) -> DataStream:
        """
        Create a new datastream.
        """
        r = self.post(
            "/stream",
            json={
                "name": name,
                "description": description,
                "type": type,
                "layer_shifts": layer_shifts,
                "datapool": datapool,
                "plugin": plugin,
                "plugin_args": plugin_args,
                "signal_reduction": signal_reduction,
                "insight_workspace": insight_workspace,
                "insight_project": insight_project,
            },
        )
        r_json = validate_response(r, "Create stream failed")
        return self.get_stream(r_json["id"])

    def delete_stream(self, stream_key: str | int) -> None:
        """
        Delete a datastream and all its datasets.

        Warning:
            This is a destructive operation that cannot be undone.
        """
        stream_id = self._get_stream_id(stream_key)
        r = self.post(f"/stream/{stream_id}/delete")
        validate_response(r, "Delete stream failed")

    def get_streams(self) -> list[DataStream]:
        """
        Get a list of all datastreams in the datapool.
        """
        self._refresh_stream_cache()
        return list(self._streams.values())

    def get_stream(self, stream_key: str | int) -> DataStream:
        """
        Get a datastream by its name or ID.

        """
        stream_id = self._get_stream_id(stream_key)
        return self._streams[stream_id]

    def _find_stream(self, stream_key: str | int) -> DataStream | None:
        if isinstance(stream_key, int):
            return self._streams.get(stream_key)
        return next(
            (
                s
                for s in self._streams.values()
                if s.name.lower() == stream_key.lower() or str(s.id) == stream_key
            ),
            None,
        )

    def _get_stream_id(self, stream_key: str | int) -> int:
        s = self._find_stream(stream_key)
        if s is not None:
            return s.id
        self._refresh_stream_cache()
        s = self._find_stream(stream_key)
        if s is not None:
            return s.id
        raise Exception(
            f"Stream with name or id {stream_key} not found, available streams: {', '.join([s.name for s in self._streams.values()])}"
        )

    def rerun_processing(self, stream_key: str | int, dataset_ids: Sequence[int] | None = None) -> None:
        """
        Rerun aliasing and processing scripts for a stream or selected datasets.

        See :meth:`~marple.db.datastream.DataStream.rerun_processing`.
        """
        self.get_stream(stream_key).rerun_processing(dataset_ids)

    def get_scripts(self) -> list[Script]:
        """List processing scripts in this workspace (without source / version history)."""
        r = self.get("/scripts")
        return [
            Script(client=self.client, **script)
            for script in validate_response(r, "Get scripts failed")["scripts"]
        ]

    def get_script(self, script_id: int) -> Script:
        """Get a processing script by ID, including recent versions and source."""
        return Script.fetch(self.client, script_id)

    def run_script(
        self,
        dataset_id: int,
        script: Script | int,
        *,
        source: str | Path | None = None,
        version: int | None = None,
        timeout: float = 180,
    ) -> Dataset:
        """Run a stored processing script on a dataset.

        See :meth:`~marple.db.dataset.Dataset.run`.

        Args:
            dataset_id: The ID of the dataset to run the script on.
            script: A stored script or its ID.
            source: Optional source text or ``.py`` path to save before running.
            version: Script version ID to run. Defaults to the latest version.
            timeout: Seconds to wait for the sandbox job to finish.

        Warning:
            This is not a dry-run. The script will be executed and the dataset will be modified.
        """
        return self.get_dataset(dataset_id).run(script, source=source, version=version, timeout=timeout)

    def create_script(
        self,
        name: str,
        script: str | Path,
        *,
        description: str | None = None,
    ) -> Script:
        """
        Create a processing script.

        Attach it to a stream with :meth:`~marple.db.datastream.DataStream.update`
        (``scripts=`` replaces the full pipeline).

        Args:
            name: The name of the script.
            script: Source text or a path to a file that must define ``process(dataset)``.
            description: The description of the script.
        """
        r = self.post(
            "/script",
            json={
                "name": name,
                "description": description,
                "script": Script.resolve_source(script),
            },
        )
        created = Script(client=self.client, **validate_response(r, "Create script failed"))
        logger.debug(f"Created script {name}")
        return created

    def delete_script(self, script_id: int) -> None:
        """
        Delete a processing script.

        Warning:
            This cannot be undone. The script is removed from every stream pipeline.
        """
        r = self.delete(f"/script/{script_id}")
        validate_response(r, "Delete script failed")
        logger.debug(f"Deleted script {script_id}")

    # Signal search and statistics #

    def search_signals(
        self,
        query: str | None = None,
        *,
        stream: StreamKey | None = None,
        sort: SignalSort | str = SignalSort.IMPORTED_COUNT,
        descending: bool = True,
        limit: int | None = 50,
    ) -> SignalStatsList:
        """
        Search signals across datasets, with statistics per signal (the Signals page).

        ``query`` uses the search language of the Signals page. Words match part of the signal
        name (``*`` is a wildcard that matches the whole name), ``field:value`` terms filter, and
        a ``-`` in front of a term negates it. Terms combine with AND::

            db.search_signals("EMS_* unit:rpm frequency>=100Hz")
            db.search_signals("is:constant -is:text", sort="cold_bytes")
            db.search_signals("datasets:10..100 imported_last>=2026-10")

        Fields: ``name``, ``unit``, ``datasets``, ``coverage``, ``points``, ``cold``, ``archived``,
        ``frequency``, ``min``, ``max``, ``heated``, ``heated_last``, ``units``, ``sources``,
        ``imported_first``, ``imported_last``, ``rank``. Flags (``is:<flag>``): ``constant``,
        ``text``, ``hot``, ``heated``, ``archived``, ``conflict``, ``alias``, ``priority``,
        ``partial``, ``default``, ``oneoff``.

        Args:
            query: Search text. ``None`` matches every signal.
            stream: Only this stream (id, name or ``DataStream``). ``None`` searches the whole datapool.
            sort: Column to sort on, see :class:`~marple.db.SignalSort`.
            descending: Sort from high to low.
            limit: Maximum number of signals to return. ``None`` returns every match.

        Returns:
            A list of :class:`~marple.db.SignalStats`. Its ``total`` is the number of matches,
            which can be more than ``limit``.

        Raises:
            ValueError: If the query does not parse; the message says which term is wrong.
            RuntimeError: If the statistics of the datapool are still being collected.
        """
        return signal_stats.search(
            self.client, query, stream=stream, sort=sort, descending=descending, limit=limit
        )

    def get_signal_stats(
        self, signal: int | str | SignalStats, *, stream: StreamKey | None = None
    ) -> SignalStatsDetail:
        """
        All statistics of one signal, including a breakdown per stream.

        Args:
            signal: The signal's id or name.
            stream: Only this stream. ``None`` covers the whole datapool.

        Raises:
            ValueError: If the signal does not exist, or has no datasets in this scope.
        """
        return signal_stats.get(self.client, signal, stream=stream)

    def get_signal_stats_summary(
        self, query: str | None = None, *, stream: StreamKey | None = None
    ) -> SignalStatsSummary:
        """
        Totals over all signals: how many there are, their storage, and how many of each kind.

        Args:
            query: Search text as for :meth:`search_signals`: the totals of the signals it finds.
            stream: Only this stream. ``None`` covers the whole datapool.
        """
        return signal_stats.get_summary(self.client, query, stream=stream)

    def export_signal_stats(
        self,
        path: str | Path,
        query: str | None = None,
        *,
        stream: StreamKey | None = None,
        sort: SignalSort | str = SignalSort.IMPORTED_COUNT,
        descending: bool = True,
    ) -> Path:
        """
        Write the signals of :meth:`search_signals` to a CSV file: all matches, not a page.

        The file has a ``priority`` column. Edit it and send the file back with
        :meth:`import_signal_priority` to set the priority of many signals at once.

        Args:
            path: File to write. A directory gets a ``signals.csv``.
            query: Search text as for :meth:`search_signals`.
            stream: Only this stream. ``None`` covers the whole datapool.
            sort: Column to sort on.
            descending: Sort from high to low.

        Returns:
            The path of the file written.
        """
        return signal_stats.export(self.client, path, query, stream=stream, sort=sort, descending=descending)

    # Signal priority #

    def set_signal_priority(
        self,
        signals: int | str | SignalStats | Iterable[int | str | SignalStats],
        high: bool = True,
        *,
        stream: StreamKey | None = None,
    ) -> SignalPriorityResult:
        """
        Give signals high import priority, or take it away with ``high=False``.

        Signals with high priority are processed first on import and placed in hot storage.
        This only affects **future** imports: datasets that are already imported do not change.
        Priority is set per stream that already has the signal; a stream that imports the signal
        for the first time later starts without priority.

        Requires an admin token.

        Args:
            signals: One signal or several, each as an id, a name or a ``SignalStats``.
            high: ``True`` for high priority, ``False`` for the default priority.
            stream: Only in this stream. ``None`` sets it in every stream that has the signal.
        """
        return signal_stats.set_priority(self.client, signals, high, stream=stream)

    def import_signal_priority(
        self, csv_path: str | Path, *, stream: StreamKey | None = None
    ) -> SignalPriorityImportResult:
        """
        Set the import priority of many signals from a CSV file.

        This is the reverse of :meth:`export_signal_stats`: export, edit the ``priority`` column,
        import. Only the columns ``name`` (or ``signal``) and ``priority`` are read. ``high`` gives
        priority, ``default`` or an empty cell takes it away, and ``partial`` leaves the signal as
        it is. Signals the file does not name are left as they are.

        Requires an admin token. Like :meth:`set_signal_priority`, this only affects future imports.

        Args:
            csv_path: The CSV file.
            stream: Only in this stream. ``None`` sets it in every stream that has the signals.
        """
        return signal_stats.import_priority(self.client, csv_path, stream=stream)

    # Aliasing rules #

    def get_aliasing_rules(
        self, signal: int | str | SignalStats | Signal | None = None, *, stream: StreamKey | None = None
    ) -> list[AliasingRule]:
        """
        List aliasing rules, optionally only those of one signal or stream.

        Args:
            signal: Only rules that involve this signal name: rules that produce it
                (``signal_name``) and rules that look for it as an original name
                (``signal_hierarchy``).
            stream: Only rules that apply to this stream: it is in the rule's ``streams``, or
                the rule applies to every stream.
        """
        return aliasing.list_rules(self.client, signal, stream=stream)

    def create_aliasing_rule(
        self,
        signal_name: str,
        signal_hierarchy: list[str],
        *,
        description: str | None = None,
        streams: Iterable[StreamKey] | None = None,
        copy_metadata: bool = False,
    ) -> AliasingRule:
        """
        Create an aliasing rule: store a signal as ``signal_name``, whatever the file calls it.

        After a dataset is imported, the names in ``signal_hierarchy`` are tried in order and
        the first one the dataset has is copied to a signal called ``signal_name``. The original
        signal stays. Rules apply at import; call :meth:`~marple.db.DataStream.rerun_processing`
        to apply them to datasets that are already imported.

        Requires an admin token.

        Args:
            signal_name: The name to store the signal under. Must be unique among the rules.
            signal_hierarchy: Original names to look for, in order of preference.
            description: Free text.
            streams: The streams the rule applies to. ``None`` or empty means every stream.
            copy_metadata: Whether the aliased signal copies the metadata of the original.
        """
        return aliasing.create_rule(
            self.client,
            signal_name,
            signal_hierarchy,
            description=description,
            streams=streams,
            copy_metadata=copy_metadata,
        )

    def upsert_aliasing_rules(
        self, rules: "str | Path | pd.DataFrame | Iterable[dict[str, Any] | AliasingRule]"
    ) -> list[AliasingRule]:
        """
        Create or update many aliasing rules in one request, matched on ``signal_name``.

        Each rule replaces an existing rule with the same ``signal_name`` as a whole, or is
        created when there is none. Rules that are not named are left as they are.

        Requires an admin token.

        Args:
            rules: A CSV file path, a DataFrame, or a list of dicts. Columns / keys:
                ``signal_name`` and ``signal_hierarchy`` (required), ``description``, ``streams``
                and ``copy_metadata``. ``signal_hierarchy`` and ``streams`` are lists, or
                comma-separated text. Streams can be given by id or name.

        Returns:
            The rules as they are stored now.
        """
        return aliasing.upsert_rules(self.client, rules)

    def delete_aliasing_rule(self, rule: int | AliasingRule) -> None:
        """
        Delete an aliasing rule, given as its id or as an ``AliasingRule``. Requires an admin token.

        Aliased signals in datasets that are already imported stay until processing is rerun.
        """
        aliasing.delete_rule(self.client, rule)

    # Metadata fields #

    def get_metadata_fields(self) -> list[MetadataField]:
        """List the metadata fields defined for this workspace."""
        return metadata_fields.list_fields(self.client)

    def get_metadata_field(self, key: int | str) -> MetadataField:
        """Get a metadata field by its id or name."""
        return metadata_fields.get_field(self.client, key)

    def create_metadata_field(
        self,
        name: str,
        *,
        description: str | None = None,
        allowed_values: Iterable[str] | None = None,
        streams: Iterable[StreamKey] | None = None,
        mandatory: bool = False,
        user_input: bool = False,
    ) -> MetadataField:
        """
        Define a metadata field for datasets. Requires an admin token.

        Args:
            name: The name of the field. Must be unique.
            description: Free text.
            allowed_values: The values the field may take. ``None`` or empty allows any value.
            streams: The streams the field belongs to. ``None`` or empty means every stream.
            mandatory: Whether a dataset must have a value for this field.
            user_input: Whether users are asked for this field when they upload a file.
        """
        return metadata_fields.create_field(
            self.client,
            name,
            description=description,
            allowed_values=allowed_values,
            streams=streams,
            mandatory=mandatory,
            user_input=user_input,
        )

    def delete_metadata_field(self, key: int | str | MetadataField) -> None:
        """
        Delete a metadata field definition, given as its id, name or ``MetadataField``.

        Requires an admin token. Metadata values that datasets already have are kept.
        """
        metadata_fields.delete_field(self.client, key)

    # Lifecycle rules #

    def get_lifecycle_rules(self, stream: StreamKey | None = None, *, effective: bool = False) -> LifecycleRules:
        """
        The lifecycle rules of the workspace, or of one stream.

        Lifecycle rules set after how many days data moves to the next storage tier. ``0`` means
        never. On a stream, ``None`` means the stream inherits the workspace value.

        Args:
            stream: The stream to get the rules of. ``None`` returns the workspace rules.
            effective: For a stream: if ``False``, its own rules, with ``None`` for every rule it
                inherits. If ``True``, the rules that apply, with inherited values filled in.
        """
        return lifecycle.get_rules(self.client, stream, effective=effective)

    def list_lifecycle_rules(self) -> list[LifecycleRules]:
        """The workspace rules first, then the rules of every stream that has rules of its own."""
        return lifecycle.list_rules(self.client)

    def set_lifecycle_rules(
        self,
        stream: StreamKey | None = None,
        *,
        hot_to_cold_days: int | None | Omitted = OMITTED,
        cold_to_archive_days: int | None | Omitted = OMITTED,
        realtime_to_cold_days: int | None | Omitted = OMITTED,
        retain_prio_signals: bool | None | Omitted = OMITTED,
    ) -> LifecycleRules:
        """
        Set lifecycle rules for the workspace, or for one stream. Requires an admin token.

        Rules that are not given keep their value. Days are ``0`` for never, or a number of days;
        the server enforces a minimum per rule and rejects lower values with a ``ValueError``.
        On a stream, ``None`` makes it inherit the workspace value again. Realtime streams are
        never archived: ``cold_to_archive_days`` and ``retain_prio_signals`` must stay ``None``.

        Args:
            stream: The stream to set the rules of. ``None`` sets the workspace rules.
            hot_to_cold_days: Days without use before a signal leaves hot storage.
            cold_to_archive_days: Days before cold data is archived (only the original file is kept).
            realtime_to_cold_days: Days before a realtime dataset is moved to cold storage.
            retain_prio_signals: Whether signals with high import priority are kept out of the archive.
        """
        return lifecycle.set_rules(
            self.client,
            stream,
            hot_to_cold_days=hot_to_cold_days,
            cold_to_archive_days=cold_to_archive_days,
            realtime_to_cold_days=realtime_to_cold_days,
            retain_prio_signals=retain_prio_signals,
        )

    def _refresh_stream_cache(self, r: Response | None = None) -> None:
        if r is None:
            r = self.get("/streams")

        self._streams.clear()
        for stream in validate_response(r, "Failed to fetch streams")["streams"]:
            try:
                self._streams[stream["id"]] = DataStream(client=self.client, **stream)
            except ValidationError as e:
                warnings.warn(f"Failed to create stream {stream['name']} (id {stream['id']}): {e}")
                continue

    def get_datasets(self, stream_key: str | int | None = None) -> DatasetList:
        """
        Get a list of datasets for a given stream key.

        If stream_key is provided, returns the datasets for the specified stream.
        If stream_key is not provided, returns all datasets in the datapool.
        """
        if stream_key is not None:
            return self.get_stream(stream_key).get_datasets()
        r = self.get(f"/datapool/{self.client.datapool}/datasets")
        datasets = validate_response(r, f"Failed to get datasets for datapool {self.client.datapool}")
        return DatasetList.from_dicts(self.client, datasets)

    def get_dataset(self, dataset_id: int | None = None, dataset_path: str | None = None) -> Dataset:
        """
        Get a dataset by its ID or path.
        """
        return Dataset.fetch(self.client, dataset_id, dataset_path)

    def get_signals(self, dataset_id: int | None = None, dataset_path: str | None = None) -> list[Signal]:
        """
        Get all signals for a dataset.
        """
        return self.get_dataset(dataset_id, dataset_path).get_signals()

    def get_signal(
        self,
        dataset_id: int | None = None,
        dataset_path: str | None = None,
        signal_name: str | None = None,
        signal_id: int | None = None,
    ) -> Signal | None:
        """
        Get a signal from a dataset.

        You can specify the signal by its name or ID and the dataset by its ID or path.
        """
        return self.get_dataset(dataset_id, dataset_path).get_signal(signal_name, signal_id)

    # SQL functions #

    @property
    def trino_info(self) -> dict:
        """
        Connection metadata for SQL querying: `host`, `user`, `hot_catalog`,
        `cold_catalog`, and `datapool`.

        Useful for building fully-qualified table names (see `query`). Not
        available on Marple SaaS.
        """
        return sql.trino_params(self.client)

    def connect_trino(self) -> "Connection":
        """
        Open a raw Trino DBAPI connection to Marple DB and return it.

        Use this for full control (server-side cursors, streaming large
        results, passing to other tools). For one-shot queries, prefer `query`.

        Connection details are discovered automatically from the API token and
        URL. SQL querying is not available on Marple SaaS.
        """
        return sql.connect_trino(self.client)

    def query(self, sql_query: str, params: Optional[Sequence[Any]] = None) -> "pd.DataFrame":
        """
        Run a Trino SQL query against Marple DB and return a pandas DataFrame.

        Hot (Postgres metadata) and cold (Iceberg raw data) are queryable as one
        database. Tables must be fully qualified; see `trino_info` for the
        catalog names. Layout:

        - Hot metadata: `<hot_catalog>.public.mdb_<datapool>_dataset`,
          `..._signal`, `..._signal_enum`.
        - Cold raw data: `<cold_catalog>.<datapool>.data` with columns
          `dataset`, `signal`, `time`, `value`, `value_text`.

        The cold `data` table is keyed by dataset/signal **IDs**, not names; join
        `..._signal_enum` (`name` -> `id`) to filter by signal name.

        Pass `params` as a sequence bound to `?` placeholders to parameterize the
        query. SQL querying is not available on Marple SaaS.

        Example:
            info = db.trino_info
            db.query(
                f"SELECT time, value FROM {info['cold_catalog']}.{info['datapool']}.data "
                "WHERE dataset = ? AND signal = ? LIMIT 1000",
                params=[12, 3],
            )

        Full schema and examples: https://docs.marpledata.com/docs/marple-db/querying
        """
        return sql.query(self.client, sql_query, params)

    # Deprecated functions #

    @deprecated
    def push_file(
        self,
        stream_key: str | int,
        file_path: str,
        metadata: dict | None = None,
        file_name: str | None = None,
        overwrite: bool = False,
        plugin_args: str | None = None,
    ) -> int:
        """
        Push a file to a datastream.
        - `stream_key`: The name or ID of the stream to push the file to.
        - `file_path`: The path to the file to be pushed.
        - `metadata`: (optional) A dictionary of metadata to be associated with the file.
        - `file_name`: (optional) The name of the file to be stored in the stream.
        - `overwrite`: (optional) If true, existing dataset with the same name will be overwritten.
        - `plugin_args`: (optional) Plugin arguments for this ingest.

        Note:
            This function is deprecated and it is encouraged to use the `push_file` method in the `DataStream` class directly.
        """
        stream = self.get_stream(stream_key)
        return stream.push_file(file_path, metadata, file_name, overwrite=overwrite, plugin_args=plugin_args).id

    @deprecated
    def get_status(self, stream_key: str | int, dataset_id: int) -> dict:
        """
        Get the status of a dataset in a stream.

        Note:
          This function is deprecated and it is encouraged to use the Dataset class directly.
        """
        stream_id = self._get_stream_id(stream_key)
        r = self.post(f"/stream/{stream_id}/datasets/status", json=[dataset_id])
        datasets = validate_response(r, "Failed to get status for dataset")["datasets"]
        for dataset in datasets:
            if dataset["dataset_id"] == dataset_id:
                return dataset

        raise Exception(f"No status found for dataset {dataset_id} in stream {stream_key}")

    @deprecated
    def download_original(self, stream_key: str | int, dataset_id: int, destination_folder: str = ".") -> Path:
        """
        Download the original file for a dataset to the destination folder.

        Note:
          This function is deprecated and it is encouraged to use the `download` method in the `Dataset` class directly.
        """
        return self.get_dataset(dataset_id).download(destination_folder)

    @deprecated
    def download_signal(
        self,
        dataset_id: int | None = None,
        dataset_path: str | None = None,
        signal_id: int | None = None,
        signal_name: str | None = None,
        refresh_cache: bool = False,
    ) -> list[Path]:
        """
        Download the parquet file for a signal from the dataset to the destination folder.

        Note:
          This function is deprecated and it is encouraged to use the `download` method in the `Signal` class directly.
        """
        signal = self.get_signal(
            dataset_id,
            dataset_path,
            signal_name,
            signal_id,
        )
        if signal is None:
            raise Exception("Signal not found")
        return signal.list_parquet_files(refresh_cache)

    def delete_dataset(self, dataset_id: int | None, dataset_path: str | None):
        """
        Delete a dataset by its ID.

        Warning:
            This is a destructive operation that cannot be undone.
        """
        dataset = self.get_dataset(dataset_id, dataset_path)
        r = self.post(f"/stream/{dataset.datastream_id}/dataset/{dataset.id}/delete")
        validate_response(r, "Delete dataset failed")

    def delete_signals(
        self,
        dataset_id: int | None,
        dataset_path: str | None,
        signal_ids: Sequence[int],
    ) -> None:
        """
        Delete signals from a dataset by ID or path.

        Warning:
            This is a destructive operation that cannot be undone.
        """
        self.get_dataset(dataset_id, dataset_path).delete_signals(signal_ids)

    @deprecated
    def update_metadata(
        self,
        dataset_id: int | None = None,
        dataset_path: str | None = None,
        metadata: dict | None = None,
        overwrite: bool = False,
    ) -> None:
        """
        Update the metadata of a dataset.

        By default, the new metadata is merged with the existing metadata.
        If `overwrite` is True, the existing metadata is replaced with the new metadata.

        Note:
          This function is deprecated and it is encouraged to use the `update_metadata` method in the `Dataset` class directly.
        """
        if metadata is None:
            metadata = {}
        self.get_dataset(dataset_id, dataset_path).update_metadata(metadata, overwrite)

    # Dataset creation and realtime ingest #

    def add_dataset(self, stream_key: str | int, dataset_name: str, metadata: dict | None = None) -> int:
        """
        Create a new empty dataset in the specified stream.

        **Live (realtime) stream**
            - Define signals with :meth:`~marple.db.dataset.Dataset.upsert_signals`
            - Push data with :meth:`~marple.db.dataset.Dataset.append`
            - Call :meth:`~marple.db.dataset.Dataset.cool` when finished to move the
              dataset to cold Parquet/Iceberg storage, then
              :meth:`~marple.db.dataset.Dataset.wait_for_import` until it is `FINISHED`.
        **File (non-live) stream**
            - Default path: use :meth:`push_file` instead of `add_dataset` — upload a
              file (e.g. CSV); Marple parses it and writes it to the data lake.
            - Custom ingestion: create an empty dataset with `add_dataset`, then upload
              signal data with :meth:`~marple.db.dataset.Dataset.add_signal` /
              :meth:`~marple.db.dataset.Dataset.add_signals` straight into the lake.
        """
        return self.get_stream(stream_key).add_dataset(dataset_name, metadata).id

    def upsert_signals(self, stream_key: str | int, dataset_id: int, signals: list[dict]) -> None:
        """
        Add signals to a dataset or update existing ones.

        Each signal in the `signals` list should be a dictionary with the following keys:
        - `signal`: Name of the signal
        - `unit`: (optional) Unit of the signal
        - `description`: (optional) Description of the signal
        - `[any metadata key]`: (optional) Any metadata value
        """
        self.get_dataset(dataset_id=dataset_id).upsert_signals(signals)

    def dataset_append(
        self,
        stream_key: str | int,
        dataset_id: int,
        data: pd.DataFrame,
        shape: Optional[Literal["wide", "long"]] = None,
    ) -> None:
        """
        Append new data to an existing dataset.

        `data` is a DataFrame with the following columns. It can be in either "long" or "wide" format. If `shape` is not specified, the format is automatically detected:

        - `"long"` format: Each row represents a single measurement for a single signal at a specific time. The following columns are expected:

          - `time`: Unix timestamp in nanoseconds.
          - `signal`: Name of the signal as a string. Signals not yet present in the dataset are automatically added. Use `upsert_signals` to set units, descriptions and metadata.
          - `value`: (optional) Value of the signal as a float or integer.
          - `value_text`: (optional) Text value of the signal as a string.
          - At least one of the `value` or `value_text` columns must be present.

        - `"wide"` format: Each row represents a single time point with multiple signals as columns. Expects at least a `time` column.

        """
        self.get_dataset(dataset_id=dataset_id).append(data, shape)

    def dataset_cool(self, stream_key: str | int, dataset_id: int) -> Dataset:
        """
        Move all realtime data to cold storage and finalize the specified realtime dataset.

        Cooling is started asynchronously on the server. Poll completion with
        `Dataset.wait_for_import`.

        Only realtime datasets in LIVE or COOLING_FAILED status can be cooled. After
        cooling completes, the dataset no longer accepts appends and
        `import_status` becomes FINISHED.

        Returns the current dataset state (typically `import_status == "COOLING"`).
        """
        return self.get_dataset(dataset_id=dataset_id).cool()
