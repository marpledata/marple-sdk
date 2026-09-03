import ast
from enum import StrEnum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, PrivateAttr, field_validator

from marple.utils import OMITTED, DBClient, Omitted, validate_response


class SandboxJobStatus(StrEnum):
    """Status of a server-side processing script run."""

    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"

    def is_terminal(self) -> bool:
        return self in (SandboxJobStatus.SUCCEEDED, SandboxJobStatus.FAILED)


class SandboxJob(BaseModel):
    """A server-side run of a stored processing script against a dataset."""

    id: int
    dataset_id: int
    stream_id: int
    script_id: int | None = None
    script_version: int | None = None
    script_index: int | None = None
    ingestion_id: int | None = None
    status: SandboxJobStatus
    batch_job_id: str | None = None
    token_id: int | None = None
    log: str | None = None
    created_by: str
    created_at: float
    started_at: float | None = None
    finished_at: float | None = None

    @classmethod
    def fetch(cls, client: DBClient, job_id: int) -> "SandboxJob":
        """Fetch a sandbox job by ID."""
        r = client.get(f"/sandbox-job/{job_id}")
        return cls.model_validate(validate_response(r, "Get sandbox job failed", check_envelope=False))


class ScriptVersion(BaseModel):
    """A stored version of a processing script's source."""

    id: int
    script: str
    updated_at: float
    updated_by: str


class Script(BaseModel):
    """
    A reusable processing script that runs on datasets after ingest.

    Scripts are stored at workspace level. Attach them to one or more streams
    with ``streams`` (appends to each stream's pipeline) or set the full
    pipeline order with :meth:`~marple.db.datastream.DataStream.update`.

    The script source must define ``process(dataset)``. List responses do not
    include ``versions``; use :meth:`fetch` / :meth:`refresh` or the object
    returned by create/update to read source code.
    """

    id: int
    name: str
    description: str | None
    created_at: float
    created_by: str
    updated_at: float
    updated_by: str
    streams: list[int] = Field(default_factory=list)
    versions: list[ScriptVersion] = Field(default_factory=list)

    _client: DBClient = PrivateAttr()

    def __init__(self, client: DBClient, **kwargs):
        super().__init__(**kwargs)
        self._client = client

    @field_validator("streams", "versions", mode="before")
    @classmethod
    def _default_list(cls, value: object) -> object:
        return value or []

    @property
    def source(self) -> str | None:
        """Latest script source, or ``None`` if versions were not loaded."""
        return self.versions[0].script if self.versions else None

    @classmethod
    def fetch(cls, client: DBClient, script_id: int) -> "Script":
        """Fetch a script by ID, including recent versions and source."""
        r = client.get(f"/script/{script_id}")
        return cls(client=client, **validate_response(r, "Get script failed"))

    def refresh(self) -> "Script":
        """Return a freshly fetched copy of this script."""
        return self.fetch(self._client, self.id)

    def update(
        self,
        *,
        name: str | Omitted = OMITTED,
        description: str | None | Omitted = OMITTED,
        script: str | Path | Omitted = OMITTED,
        streams: list[int] | Omitted = OMITTED,
    ) -> "Script":
        """
        Update this script. Only provided fields are sent.

        Args:
            name: The new name for the script.
            description: The new description for the script.
            script: Source text, a ``.py`` path string, or a :class:`~pathlib.Path`.
            streams: The new streams to attach the script to.
        """
        payload: dict[str, Any] = {}
        if name is not OMITTED:
            payload["name"] = name
        if description is not OMITTED:
            payload["description"] = description
        if script is not OMITTED:
            payload["script"] = self.resolve_source(script)
        if streams is not OMITTED:
            payload["streams"] = streams

        r = self._client.post(f"/script/{self.id}", json=payload)
        return Script(client=self._client, **validate_response(r, "Update script failed"))

    def duplicate(self) -> "Script":
        """Create a copy of this script, including version history and stream attachments."""
        r = self._client.post(f"/script/{self.id}/duplicate")
        return Script(client=self._client, **validate_response(r, "Duplicate script failed"))

    def delete(self) -> None:
        """
        Delete this script.

        Warning:
            This cannot be undone. The script is removed from every stream pipeline.
        """
        r = self._client.delete(f"/script/{self.id}")
        validate_response(r, "Delete script failed")

    @staticmethod
    def resolve_source(script: Path | str) -> str:
        """
        Resolve script source from text or a file.

        Args:
            script: A source text or a path to a file that must define ``process(dataset)``.
        """
        if isinstance(script, Path) or script.endswith(".py"):
            path = Path(script)
            source = path.read_text()
            filename = str(path)
        else:
            source = script
            filename = "<script>"

        try:
            tree = ast.parse(source, filename=filename)
        except SyntaxError as e:
            where = f"line {e.lineno}" if e.lineno else filename
            raise ValueError(f"Invalid script: {e.msg} ({where})") from e

        named = [node for node in tree.body if getattr(node, "name", None) == "process"]
        if len(named) != 1:
            raise ValueError("Script must define exactly one process(dataset)")
        node = named[0]
        if not isinstance(node, ast.FunctionDef):
            raise ValueError(f"process() must be a (synchronous) function, found {type(node).__name__} instead")
        if len(node.args.args) != 1:
            raise ValueError("process() must take exactly one argument")
        return source
