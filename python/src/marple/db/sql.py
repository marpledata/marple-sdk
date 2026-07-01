"""Trino SQL querying for Marple DB.

Connection parameters are discovered automatically from the API token, the API
URL, and the ``/workspace/license`` endpoint. Querying is currently disabled on
Marple SaaS.
"""

from typing import TYPE_CHECKING, Any, Optional, Sequence
from urllib.parse import urlparse

import pandas as pd
from trino.auth import BasicAuthentication
from trino.dbapi import Connection, connect

from marple.db.constants import SAAS_URL
from marple.utils import validate_response

if TYPE_CHECKING:
    from marple.utils import DBClient

TRINO_PORT = 443
DOCS_URL = "https://docs.marpledata.com/docs/marple-db/querying"


def _check_saas(client: "DBClient") -> None:
    if client.api_url.rstrip("/") == SAAS_URL.rstrip("/"):
        raise RuntimeError(
            "SQL querying via Trino is not available on Marple SaaS yet; "
            "it is currently only enabled for VPC and self-hosted deployments."
        )


def trino_params(client: "DBClient") -> dict[str, Any]:
    """Discover and cache the Trino connection parameters for this client."""
    _check_saas(client)
    if client._trino_cache is not None:
        return client._trino_cache

    r = client.get("/workspace/license")
    workspace = validate_response(r, "Failed to fetch workspace license")["workspace"]
    api_host = urlparse(client.api_url).hostname or ""
    params = {
        "host": client.trino_host or f"query.{api_host}",
        "port": TRINO_PORT,
        "user": f"mdb_{workspace}",
        "hot_catalog": f"mdb_{workspace}_hot",
        "cold_catalog": f"mdb_{workspace}_cold",
        "datapool": client.datapool,
    }
    client._trino_cache = params
    return params


def connect_trino(client: "DBClient") -> Connection:
    """Open a raw Trino DBAPI connection reaching both the hot and cold catalogs."""
    p = trino_params(client)
    return connect(
        host=p["host"],
        port=p["port"],
        http_scheme="https",
        user=p["user"],
        auth=BasicAuthentication(p["user"], client.api_token),
    )


def query(client: "DBClient", sql: str, params: Optional[Sequence[Any]] = None) -> pd.DataFrame:
    """Run a Trino SQL query and return the result as a pandas DataFrame."""
    conn = connect_trino(client)
    try:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description] if cursor.description else []
    except Exception as e:
        raise _map_trino_error(e)
    finally:
        conn.close()
    return pd.DataFrame(rows, columns=columns)


def _map_trino_error(error: Exception) -> Exception:
    text = str(error)
    if any(token in text for token in ("401", "403", "Unauthorized", "Forbidden")):
        return RuntimeError(
            "Trino rejected the connection. Verify the API token is valid and that "
            "SQL querying is enabled for this workspace. See "
            f"{DOCS_URL}"
        )
    return error
