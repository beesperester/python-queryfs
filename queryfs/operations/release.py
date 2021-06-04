import os

from pathlib import Path
from typing import Optional
from time import time

from queryfs.core import Core
from queryfs.db.session import Constraint
from queryfs.schemas import File, Directory, Filenode
from queryfs.hashing import hash_from_file


def op_release(core: Core, path: str, fh: int) -> None:
    original_path = Path(str(path)[1:])
    file_name = os.path.basename(path)
    result = core.resolve_path(path)

    if isinstance(result, File):
        filenode_instance = result.filenode(core.session)

        if filenode_instance:
            resolved_path = core.blobs.joinpath(filenode_instance.hash)
        else:
            raise Exception(
                f"Missing Filenode for file '{result.id}' at '{original_path}'"
            )
    elif isinstance(result, Directory):
        raise Exception(f"Trying to release directory at '{original_path}'")
    else:
        resolved_path = result

    os.close(fh)

    if fh in core.writable_file_handles:
        # remove file handle from list of writable file handles
        core.writable_file_handles.remove(fh)

        # create hash from file
        hash = hash_from_file(resolved_path)

        if hash != core.empty_hash:
            print(f"releasing '{original_path}' as '{resolved_path}'")

            ctime = time()

            size = resolved_path.stat().st_size

            directory_id: Optional[int] = None

            directory_instance = core.resolve_db_entity(
                str(original_path.parent)
            )

            if directory_instance:
                directory_id = directory_instance.id

            constraints = [
                Constraint("name", "=", file_name),
                Constraint("directory_id", "is", directory_id),
            ]

            print(constraints)

            file_instance = (
                core.session.query(File)
                .select()
                .where(
                    *constraints,
                )
                .execute()
                .fetch_one()
            )

            if file_instance:
                filenode_instance = file_instance.filenode(core.session)

                if not filenode_instance:
                    raise Exception("Missing Filenode")

                # udpate existing file
                previous_hash = filenode_instance.hash

                core.session.query(Filenode).update(
                    hash=hash, atime=ctime, mtime=ctime, size=size
                ).where(
                    Constraint("id", "is", filenode_instance.id)
                ).execute().close()

                print(f"updated existing filenode '{filenode_instance.id}'")

                # remove pointless blobs
                pointers = (
                    core.session.query(Filenode)
                    .select("id")
                    .where(Constraint("hash", "=", previous_hash))
                    .execute()
                    .fetch_all()
                )

                if not pointers:
                    print(f"no pointers pointing to blob '{previous_hash}'")

                    previous_blob_path = core.blobs.joinpath(previous_hash)

                    if previous_blob_path.is_file():
                        os.unlink(previous_blob_path)
            else:
                # insert new filenode
                filenode_id = (
                    core.session.query(Filenode)
                    .insert(
                        hash=hash,
                        ctime=ctime,
                        atime=ctime,
                        mtime=ctime,
                        size=size,
                    )
                    .execute()
                    .get_last_row_id()
                )

                print(f"inserted new filenode '{filenode_id}'")

                # insert new file
                file_id = (
                    core.session.query(File)
                    .insert(
                        name=file_name,
                        directory_id=directory_id,
                        filenode_id=filenode_id,
                    )
                    .execute()
                    .get_last_row_id()
                )

                print(f"inserted new file '{file_id}'")

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
