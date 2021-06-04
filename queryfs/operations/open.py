import os
import errno

from pathlib import Path
import shutil
from queryfs.models.filenode import Filenode
from typing import Optional
from fuse import FuseOSError

from queryfs.core import Core
from queryfs.models.file import File
from queryfs.models.directory import Directory


def op_open(core: Core, path: str, flags: int) -> int:
    original_path = Path(str(path)[1:])
    # file_name = os.path.basename(path)
    result = core.resolve_path(path)
    filenode_instance: Optional[Filenode] = None

    if isinstance(result, File):
        filenode_instance = result.filenode(core.session)

        if filenode_instance:
            resolved_path = core.blobs.joinpath(filenode_instance.hash)
        else:
            raise Exception(
                f"Missing Filenode for file '{result.id}' at '{original_path}'"
            )
    elif isinstance(result, Directory):
        # resolved_path = core.temp
        raise Exception(f"Trying to open directory at '{original_path}'")
    else:
        resolved_path = result

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

            print(
                f"opening '{original_path}' as '{resolved_path}' with flags '{flags}'"
            )

            return fh

    # try and open file from blobs diretory
    # file_instance = (
    #     core.session.query(File)
    #     .select()
    #     .where(Constraint("name", "=", file_name))
    #     .execute()
    #     .fetch_one()
    # )

    # if file_instance:
    #     filenode_instance = fetch_filenode(core.session, file_instance)

    if filenode_instance:
        blob_path = core.blobs.joinpath(filenode_instance.hash)
        if flags == 0:

            # readable blob file
            fh = os.open(blob_path, flags)

            return fh
        else:
            # new writable temp file
            temp_path = core.temp.joinpath(original_path)

            if not temp_path.parent.is_dir():
                os.makedirs(temp_path.parent, exist_ok=True)

            # copy blob file to temp file
            shutil.copyfile(blob_path, temp_path)

            # flags = os.O_WRONLY | os.O_CREAT

            fh = os.open(temp_path, flags)

            if fh not in core.writable_file_handles:
                core.writable_file_handles.append(fh)

            print(
                f"opening '{original_path}' as '{temp_path}' with flags '{flags}'"
            )

            return fh

    raise FuseOSError(errno.ENOENT)
