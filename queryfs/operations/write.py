import os
import logging

from queryfs.logging import format_entry
from queryfs.repository import Repository

logger = logging.getLogger("operations")


def op_write(
    repository: Repository, path: str, data: bytes, offset: int, fh: int
) -> int:
    os.lseek(fh, offset, 0)

    logger.info(
        format_entry(
            "op_write", path=path, data=len(data), offset=offset, fh=fh
        )
    )

    return os.write(fh, data)
