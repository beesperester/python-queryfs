import os
import logging

from queryfs.logging import format_entry
from queryfs.repository import Repository

logger = logging.getLogger("operations")


def op_flush(repository: Repository, path: str, fh: int) -> None:
    logger.info(format_entry("op_flush", path=path, fh=fh))

    return os.fsync(fh)
