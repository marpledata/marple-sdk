import types

import pandas as pd
import pytest

import marple
from marple import DB
from marple.db import sql


def _is_saas(url: str) -> bool:
    return url.rstrip("/") == marple.db.SAAS_URL.rstrip("/")


def test_saas_guard_blocks_querying() -> None:
    fake_client = types.SimpleNamespace(api_url=marple.db.SAAS_URL)
    with pytest.raises(RuntimeError, match="SaaS"):
        sql.trino_params(fake_client)  # type: ignore[arg-type]


@pytest.fixture()
def trino_db(db: DB) -> DB:
    if _is_saas(db.client.api_url):
        pytest.skip("SQL querying is disabled on Marple SaaS")
    return db


def test_trino_info_exposes_catalogs(trino_db: DB) -> None:
    info = trino_db.trino_info
    assert set(info) >= {"host", "user", "hot_catalog", "cold_catalog", "datapool"}
    assert info["hot_catalog"].endswith("_hot")
    assert info["cold_catalog"].endswith("_cold")
    assert info["user"].startswith("mdb_")


def test_query_returns_dataframe(trino_db: DB) -> None:
    df = trino_db.query("SHOW CATALOGS")
    assert isinstance(df, pd.DataFrame)
    catalogs = set(df.iloc[:, 0])
    info = trino_db.trino_info
    assert info["hot_catalog"] in catalogs
    assert info["cold_catalog"] in catalogs


def test_query_with_params(trino_db: DB) -> None:
    df = trino_db.query("SELECT ? AS a, ? AS b", params=[1, 2])
    assert list(df.iloc[0]) == [1, 2]
