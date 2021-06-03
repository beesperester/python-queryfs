import os
import errno

from typing import Dict, Any, Optional, Union, List, Tuple
from pathlib import Path
from fuse import FuseOSError

from queryfs.core import Core
from queryfs.db.session import Constraint, Session
from queryfs.models.file import File
from queryfs.models.directory import Directory


def op_open(core: Core, path: str, flags: int) -> int:
    original_path = Path(str(path)[1:])
    file_name = os.path.basename(path)
    result = core.resolve_path(path)

    if isinstance(result, File):
        resolved_path = core.blobs.joinpath(result.hash)
    elif isinstance(result, Directory):
        resolved_path = core.temp
    else:
        resolved_path = result

    if flags > 0:
        print(resolved_path)

    # try and open file from temp directory
    if str(resolved_path).startswith(str(core.temp)):
        if flags == 0:
            # readable temp file
            fh = os.open(resolved_path, flags)

            return fh
        else:
            if not resolved_path.parent.is_dir():
                os.makedirs(resolved_path.parent, exist_ok=True)

            # writable temp file
            fh = os.open(resolved_path, flags)

            # update fh for file in db
            if fh not in core.writable_file_handles:
                core.writable_file_handles.append(fh)

            return fh

    # try and open file from blobs diretory
    file_instance = (
        core.session.query(File)
        .select()
        .where(Constraint("name", "=", file_name))
        .execute()
        .fetch_one()
    )

    if file_instance:
        if flags == 0:
            # readable blob file
            blob_path = core.blobs.joinpath(file_instance.hash)

            fh = os.open(blob_path, flags)

            return fh
        else:
            # new writable temp file
            temp_path = core.temp.joinpath(original_path)

            if not temp_path.parent.is_dir():
                os.makedirs(temp_path.parent, exist_ok=True)

            flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC

            fh = os.open(temp_path, flags)

            if fh not in core.writable_file_handles:
                core.writable_file_handles.append(fh)

            return fh
    else:
        raise FuseOSError(errno.ENOENT)