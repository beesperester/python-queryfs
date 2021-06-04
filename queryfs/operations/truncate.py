import os
import logging

from typing import Optional

from queryfs.logging import format_entry
from queryfs.core import Core

logger = logging.getLogger("operations")


def op_truncate(
    core: Core, path: str, length: int, fh: Optional[int] = None
) -> None:
    result = core.resolve_path(path)

    logger.info(
        format_entry(
            "op_truncate", path=path, length=length, fh=fh, resolved=result
        )
    )

    if fh in core.writable_file_handles:
        os.truncate(fh, length)
    else:
        logger.warning(
            format_entry(
                "op_truncate",
                "file handle not in list of writable file handles",
            )
        )
