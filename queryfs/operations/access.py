import os
import errno
import logging

from fuse import FuseOSError

from queryfs.logging import format_entry
from queryfs.core import Core
from queryfs.schemas import File, Directory

logger = logging.getLogger("operations")


def op_access(core: Core, path: str, amode: int) -> None:
    result = core.resolve_path(path)

    logger.info(
        format_entry("op_access", path=path, amode=amode, resolved=result)
    )

    if isinstance(result, File):
        filenode_instance = result.filenode(core.session)

        if filenode_instance:
            resolved_path = core.blobs.joinpath(filenode_instance.hash)

            logger.info(
                format_entry("op_access", "load blob", path=resolved_path)
            )
        else:
            raise Exception("Missing Filenode")
    elif isinstance(result, Directory):
        return
    else:
        resolved_path = result

    if not os.access(resolved_path, amode):
        raise FuseOSError(errno.EACCES)
