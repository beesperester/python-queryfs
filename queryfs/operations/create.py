import os
import errno

from typing import Optional
from fuse import FuseOSError

from queryfs.core import Core
from queryfs.schemas import File, Directory


def op_create(
    core: Core, path: str, mode: int, fi: Optional[bool] = None
) -> int:
    # file_name = os.path.basename(path)
    result = core.resolve_path(path)

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

    if fh not in core.writable_file_handles:
        core.writable_file_handles.append(fh)

    return fh