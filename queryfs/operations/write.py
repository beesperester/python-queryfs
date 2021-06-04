import os
import logging

from queryfs.logging import format_entry
from queryfs.core import Core

logger = logging.getLogger("operations")


def op_write(core: Core, path: str, data: bytes, offset: int, fh: int) -> int:
    os.lseek(fh, offset, 0)

    logger.info(
        format_entry(
            "op_write", path=path, data=len(data), offset=offset, fh=fh
        )
    )

    return os.write(fh, data)
