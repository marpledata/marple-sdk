import logging
import sys
from typing import TextIO

logger = logging.getLogger("marple.sdk")
logger.addHandler(logging.NullHandler())
logger.propagate = False

_handler: logging.Handler | None = None


def verbose(enabled: bool = True, file: TextIO | None = None) -> None:
    """Turn SDK mutation activity logging on or off (stdout by default)."""
    global _handler
    if enabled:
        logger.setLevel(logging.DEBUG)
        if _handler is not None:
            return
        _handler = logging.StreamHandler(file or sys.stdout)
        _handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(_handler)
        return

    if _handler is not None:
        logger.removeHandler(_handler)
        _handler.close()
        _handler = None
    logger.setLevel(logging.NOTSET)
