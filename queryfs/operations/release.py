import os

from pathlib import Path
from queryfs.models.filenode import Filenode
from time import time

from queryfs.core import Core
from queryfs.db.session import Constraint
from queryfs.models.file import File, fetch_filenode
from queryfs.models.directory import Directory
from queryfs.hashing import hash_from_file


def op_release(core: Core, path: str, fh: int) -> None:
    original_path = Path(str(path)[1:])
    file_name = os.path.basename(path)
    result = core.resolve_path(path)

    if isinstance(result, File):
        filenode_instance = fetch_filenode(core.session, result)

        if filenode_instance:
            resolved_path = core.blobs.joinpath(filenode_instance.hash)
        else:
            raise Exception("Missing Filenode")
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
                filenode_instance = fetch_filenode(core.session, file_instance)

                if not filenode_instance:
                    raise Exception("Missing Filenode")

                # udpate existing file
                previous_hash = filenode_instance.hash

                core.session.query(Filenode).update(
                    hash=hash, atime=ctime, mtime=ctime, size=size
                ).where(
                    Constraint("id", "=", filenode_instance.id)
                ).execute().close()

                # core.session.query(File).update(
                #     hash=hash, atime=ctime, mtime=ctime, size=size
                # ).where(
                #     Constraint("id", "=", file_instance.id)
                # ).execute().close()

                # remove pointless blobs
                pointers = (
                    core.session.query(Filenode)
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

                # insert new file
                core.session.query(File).insert(
                    name=file_name,
                    directory_id=directory_id,
                    filenode_id=filenode_id,
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
