import os
import errno
import logging

from typing import Optional
from fuse import FuseOSError

from queryfs.logging import format_entry
from queryfs.repository import Repository
from queryfs.schemas import File, Directory

logger = logging.getLogger("operations")


def op_create(
    repository: Repository, path: str, mode: int, fi: Optional[bool] = None
) -> int:
    # file_name = os.path.basename(path)
    result = repository.resolve_path(path)

    logger.info(
        format_entry("op_create", path=path, mode=mode, fi=fi, resolved=result)
    )

    if isinstance(result, File):
        raise FuseOSError(errno.EACCES)
    elif isinstance(result, Directory):
        raise FuseOSError(errno.EACCES)
    else:
        resolved_path = result

    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC

    if not resolved_path.parent.is_dir():
        os.makedirs(resolved_path.parent, exist_ok=True)

    fh = os.open(resolved_path, flags, mode)

    if fh not in repository.writable_file_handles:
        repository.writable_file_handles.append(fh)

    return fh