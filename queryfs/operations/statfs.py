import os
import logging

from typing import Dict, Any

from queryfs.logging import format_entry
from queryfs.repository import Repository
from queryfs.schemas import File, Directory

logger = logging.getLogger("operations")


def op_statfs(repository: Repository, path: str) -> Dict[str, Any]:
    result = repository.resolve_path(path)

    logger.info(format_entry("op_statfs", path=path, resolved=result))

    if isinstance(result, File):
        filenode_instance = result.filenode(repository.session)

        if filenode_instance:
            resolved_path = repository.blobs.joinpath(filenode_instance.hash)
        else:
            raise Exception("Missing Filenode")
    elif isinstance(result, Directory):
        resolved_path = repository.temp
    else:
        resolved_path = result

    key_names = [
        "f_bavail",
        "f_bfree",
        "f_blocks",
        "f_bsize",
        "f_favail",
        "f_ffree",
        "f_files",
        "f_flag",
        "f_frsize",
        "f_namemax",
    ]

    stv = os.statvfs(resolved_path)

    result = {key: getattr(stv, key) for key in key_names}

    return result
