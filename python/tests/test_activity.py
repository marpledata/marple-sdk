from io import StringIO
from unittest.mock import patch

import pytest

from marple.db import DB
from marple.db.activity import logger, verbose


@pytest.fixture(autouse=True)
def _reset_verbose():
    verbose(False)
    yield
    verbose(False)


def test_verbose_writes_debug_to_file():
    buf = StringIO()
    verbose(True, buf)
    logger.debug("hello activity")
    assert "hello activity" in buf.getvalue()


def test_verbose_true_is_idempotent():
    buf = StringIO()
    verbose(True, buf)
    verbose(True, buf)
    logger.debug("once")
    assert buf.getvalue().count("once") == 1


def test_verbose_false_stops_output():
    buf = StringIO()
    verbose(True, buf)
    logger.debug("before")
    verbose(False)
    logger.debug("after")
    assert "before" in buf.getvalue()
    assert "after" not in buf.getvalue()


def test_db_verbose_delegates():
    with patch.object(DB, "check_connection", return_value=True):
        with patch("marple.utils.DBClient"):
            db = DB("token")
    buf = StringIO()
    db.verbose(True, buf)
    logger.debug("via db")
    assert "via db" in buf.getvalue()
    db.verbose(False)
    logger.debug("off")
    assert "off" not in buf.getvalue()
