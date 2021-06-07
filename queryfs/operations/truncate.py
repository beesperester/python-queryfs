import os
import logging

from typing import Optional

from queryfs.logging import format_entry
from queryfs.repository import Repository

logger = logging.getLogger("operations")


def op_truncate(
    repository: Repository, path: str, length: int, fh: Optional[int] = None
) -> None:
    result = repository.resolve_path(path)

    logger.info(
        format_entry(
            "op_truncate", path=path, length=length, fh=fh, resolved=result
        )
    )

    if fh in repository.writable_file_handles:
        os.truncate(fh, length)
    else:
        logger.warning(
            format_entry(
                "op_truncate",
                "file handle not in list of writable file handles",
            )
        )
