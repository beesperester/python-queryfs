import errno
import os
import shutil

from fuse import FuseOSError

from pathlib import Path
from typing import Optional

from queryfs.core import Core
from queryfs.schemas import File, Directory


def op_truncate(
    core: Core, path: str, length: int, fh: Optional[int] = None
) -> None:
    original_path = Path(path[1:])

    result = core.resolve_path(path)

    # if fh in core.writable_file_handles:
    #     # remove file handle from list of writable file handles
    #     core.writable_file_handles.remove(fh)

    #     print(f"truncate '{original_path}' with fh '{fh}'")

    #     os.truncate(fh, length)

    if isinstance(result, File):
        filenode_instance = result.filenode(core.session)

        if filenode_instance:
            blob_path = core.blobs.joinpath(filenode_instance.hash)
            temp_path = core.temp.joinpath(original_path)

            if not temp_path.parent.is_dir():
                os.makedirs(temp_path.parent, exist_ok=True)

            shutil.copyfile(blob_path, temp_path)

            resolved_path = temp_path
        else:
            raise Exception("Missing Filenode")
    elif isinstance(result, Directory):
        raise FuseOSError(errno.ENOENT)
    else:
        resolved_path = result

    print(f"truncate '{original_path}' as '{resolved_path}' with fh '{fh}'")

    if str(resolved_path).startswith(str(core.temp)):
        os.truncate(resolved_path, length)
