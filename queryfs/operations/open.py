import os
import errno
import logging

from pathlib import Path
import shutil
from typing import Optional
from fuse import FuseOSError

from queryfs.logging import format_entry
from queryfs.core import Core
from queryfs.schemas import File, Directory, Filenode

logger = logging.getLogger("operations")


def op_open(core: Core, path: str, flags: int) -> int:
    original_path = Path(str(path)[1:])
    # file_name = os.path.basename(path)
    result = core.resolve_path(path)
    filenode_instance: Optional[Filenode] = None

    logger.info(
        format_entry("op_open", path=path, flags=flags, resolved=result)
    )

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

            logger.info(
                format_entry(
                    "op_open",
                    "opened readable temp file",
                    path=resolved_path,
                    fh=fh,
                )
            )

            return fh
        else:
            if not resolved_path.parent.is_dir():
                os.makedirs(resolved_path.parent, exist_ok=True)

            # writable temp file
            fh = os.open(resolved_path, flags)

            # update fh for file in db
            if fh not in core.writable_file_handles:
                core.writable_file_handles.append(fh)

            logger.info(
                format_entry(
                    "op_open",
                    "opened writable temp file",
                    path=resolved_path,
                    flags=flags,
                    fh=fh,
                )
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

            logger.info(
                format_entry(
                    "op_open",
                    "opened readable blob file",
                    path=blob_path,
                    fh=fh,
                )
            )

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

            logger.info(
                format_entry(
                    "op_open",
                    "opened writable temp file from blob",
                    path=temp_path,
                    fh=fh,
                    blob_path=blob_path,
                )
            )

            return fh

    raise FuseOSError(errno.ENOENT)
