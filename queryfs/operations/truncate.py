import errno
import os

from fuse import FuseOSError
from queryfs.models.directory import Directory
import shutil

from pathlib import Path
from typing import Optional

from queryfs.models.file import File
from queryfs.core import Core


def op_truncate(
    core: Core, path: str, length: int, fh: Optional[int] = None
) -> None:
    original_path = Path(path[1:])

    result = core.resolve_path(path)

    if isinstance(result, File):
        blob_path = core.blobs.joinpath(result.hash)
        temp_path = core.temp.joinpath(original_path)

        if not temp_path.parent.is_dir():
            os.makedirs(temp_path.parent, exist_ok=True)

        shutil.copyfile(blob_path, temp_path)

        resolved_path = temp_path
    elif isinstance(result, Directory):
        raise FuseOSError(errno.ENOENT)
    else:
        resolved_path = result

    os.truncate(resolved_path, length)
