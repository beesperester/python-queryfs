import os
import logging

from queryfs.logging import format_entry
from queryfs.core import Core

logger = logging.getLogger("operations")


def op_flush(core: Core, path: str, fh: int) -> None:
    logger.info(format_entry("op_flush", path=path, fh=fh))

    return os.fsync(fh)
