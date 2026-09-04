"""Silent-by-default debug logger for SDK mutations.

``marple.sdk`` does not propagate, so ``logging.basicConfig(level=DEBUG)`` in a
notebook still prints nothing. Call :func:`enable_activity_log` (the sandbox
runner does this) to print one ``logger.debug`` line per successful mutation.
"""

from __future__ import annotations

import logging
import sys
from typing import TextIO

logger = logging.getLogger("marple.sdk")
logger.addHandler(logging.NullHandler())
logger.propagate = False


def enable_activity_log(stream: TextIO | None = None) -> None:
    """Attach a stdout handler and set ``marple.sdk`` to DEBUG."""
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(stream or sys.stdout)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
