import os
import logging

from queryfs.logging import format_entry
from queryfs.repository import Repository

logger = logging.getLogger("operations")


def op_read(
    repository: Repository, path: str, size: int, offset: int, fh: int
) -> bytes:
    os.lseek(fh, offset, 0)

    logger.info(
        format_entry("op_read", path=path, size=size, offset=offset, fh=fh)
    )

    return os.read(fh, size)
