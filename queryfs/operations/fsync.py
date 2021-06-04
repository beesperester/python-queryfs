import os
import logging

from queryfs.logging import format_entry
from queryfs.core import Core

logger = logging.getLogger("operations")


def op_fsync(core: Core, path: str, datasync: int, fh: int) -> None:
    logger.info(format_entry("op_fsync", path=path, fh=fh))

    return os.fsync(fh)
