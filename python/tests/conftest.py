import os
from collections.abc import Generator
from datetime import datetime

import dotenv
import pytest

import marple
from marple import DB, Insight
from marple.db import Dataset, DataStream
from support import ingest_dataset

dotenv.load_dotenv()


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None:
        pytest.skip(f"Missing env var {name}; skipping integration test.")
    return value


@pytest.fixture(scope="session")
def db() -> DB:
    url = os.getenv("MDB_URL", marple.db.SAAS_URL)
    assert url is not None
    return DB(_required_env("MDB_TOKEN"), url)


@pytest.fixture(scope="session")
def insight() -> Insight:
    url = os.getenv("INSIGHT_URL", marple.insight.SAAS_URL)
    assert url is not None
    return Insight(_required_env("INSIGHT_TOKEN"), api_url=url)


@pytest.fixture(scope="session")
def example_stream(db: DB) -> Generator[DataStream, None, None]:
    name = "Salty Compulsory Pytest " + datetime.now().isoformat()
    yield db.create_stream(name)
    print("Cleaning up stream...")
    db.delete_stream(name)


@pytest.fixture(scope="session")
def example_dataset(example_stream: DataStream) -> Dataset:
    return ingest_dataset(example_stream, metadata={"A": 1, "B": 1})
