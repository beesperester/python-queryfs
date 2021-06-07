import os
import errno
import logging

from fuse import FuseOSError

from queryfs.logging import format_entry
from queryfs.repository import Repository
from queryfs.schemas import File, Directory

logger = logging.getLogger("operations")


def op_access(repository: Repository, path: str, amode: int) -> None:
    result = repository.resolve_path(path)

    logger.info(
        format_entry("op_access", path=path, amode=amode, resolved=result)
    )

    if isinstance(result, File):
        filenode_instance = result.filenode(repository.session)

        if filenode_instance:
            resolved_path = repository.blobs.joinpath(filenode_instance.hash)

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
