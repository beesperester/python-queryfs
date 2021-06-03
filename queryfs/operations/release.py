import os

from typing import Dict, Any, Optional, Union, List, Tuple
from pathlib import Path
from time import time

from queryfs.core import Core
from queryfs.db.session import Constraint, Session
from queryfs.models.file import File
from queryfs.models.directory import Directory
from queryfs.hashing import hash_from_file


def op_release(core: Core, path: str, fh: int) -> None:
    original_path = Path(str(path)[1:])
    file_name = os.path.basename(path)
    result = core.resolve_path(path)

    if isinstance(result, File):
        resolved_path = core.blobs.joinpath(result.hash)
    elif isinstance(result, Directory):
        resolved_path = core.temp
    else:
        resolved_path = result

    os.close(fh)

    if fh in core.writable_file_handles:
        # remove file handle from list of writable file handles
        core.writable_file_handles.remove(fh)

        # create hash from file
        hash = hash_from_file(resolved_path)

        if hash != core.empty_hash:
            ctime = time()

            size = resolved_path.stat().st_size

            file_instance = (
                core.session.query(File)
                .select()
                .where(Constraint("name", "=", file_name))
                .execute()
                .fetch_one()
            )

            if file_instance:
                # udpate existing file
                previous_hash = file_instance.hash

                core.session.query(File).update(
                    hash=hash, atime=ctime, mtime=ctime, size=size
                ).where(
                    Constraint("id", "=", file_instance.id)
                ).execute().close()

                # remove pointless blobs
                pointers = (
                    core.session.query(File)
                    .select("id")
                    .where(Constraint("hash", "=", previous_hash))
                    .execute()
                    .fetch_all()
                )

                if not pointers:
                    previous_blob_path = core.blobs.joinpath(previous_hash)

                    if previous_blob_path.is_file():
                        os.unlink(previous_blob_path)
            else:
                directory_id = None
                parent_directory_instance = core.resolve_db_entity(
                    str(original_path.parent)
                )

                if parent_directory_instance:
                    directory_id = parent_directory_instance.id

                # insert new file
                core.session.query(File).insert(
                    name=file_name,
                    hash=hash,
                    ctime=ctime,
                    atime=ctime,
                    mtime=ctime,
                    size=size,
                    directory_id=directory_id,
                ).execute().close()

            # move temp file to blobs if not exist
            blob_path = core.blobs.joinpath(hash)

            if not blob_path.is_file():
                # move temp file to blobs
                # if no blob exists for that hash
                os.rename(resolved_path, blob_path)
            else:
                # unlink temp file
                # if a blob exists for that hash
                os.unlink(resolved_path)
