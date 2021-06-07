import os
import logging

from queryfs.logging import format_entry
from queryfs.repository import Repository

logger = logging.getLogger("operations")


def op_fsync(
    repository: Repository, path: str, datasync: int, fh: int
) -> None:
    logger.info(format_entry("op_fsync", path=path, fh=fh))

    return os.fsync(fh)
