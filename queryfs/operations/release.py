import os
import logging

from pathlib import Path
from typing import Optional
from time import time

from queryfs.logging import format_entry
from queryfs.repository import Repository
from queryfs.db import Constraint
from queryfs.schemas import File, Directory, Filenode
from queryfs.hashing import hash_from_file

logger = logging.getLogger("operations")


def op_release(repository: Repository, path: str, fh: int) -> None:
    original_path = Path(str(path)[1:])
    file_name = os.path.basename(path)
    result = repository.resolve_path(path)

    logger.info(format_entry("op_release", path=path, fh=fh, resolved=result))

    if isinstance(result, File):
        filenode_instance = result.filenode(repository.session)

        if filenode_instance:
            resolved_path = repository.blobs.joinpath(filenode_instance.hash)
        else:
            raise Exception(
                f"Missing Filenode for file '{result.id}' at '{original_path}'"
            )
    elif isinstance(result, Directory):
        raise Exception(f"Trying to release directory at '{original_path}'")
    else:
        resolved_path = result

    os.close(fh)

    if fh in repository.writable_file_handles:
        # remove file handle from list of writable file handles
        repository.writable_file_handles.remove(fh)

        logger.info(format_entry("op_release", "released", fh=fh))

        # create hash from file
        hash = hash_from_file(resolved_path)

        if hash != repository.empty_hash:
            logger.info(
                format_entry(
                    "op_release",
                    f"released '{original_path}' as '{resolved_path}'",
                )
            )

            ctime = time()

            size = resolved_path.stat().st_size

            directory_id: Optional[int] = None

            directory_instance = repository.resolve_db_entity(
                str(original_path.parent)
            )

            if directory_instance:
                directory_id = directory_instance.id

            file_instance = (
                repository.session.query(File)
                .select()
                .where(
                    Constraint("name", "=", file_name),
                    Constraint("directory_id", "is", directory_id),
                )
                .execute()
                .fetch_one()
            )

            if file_instance:
                filenode_instance = file_instance.filenode(repository.session)

                if not filenode_instance:
                    raise Exception("Missing Filenode")

                # udpate existing file
                previous_hash = filenode_instance.hash

                filenode_instance.update(
                    repository.session,
                    hash=hash,
                    atime=ctime,
                    mtime=ctime,
                    size=size,
                )

                logger.info(
                    format_entry(
                        "op_release",
                        f"updated existing filenode '{filenode_instance}'",
                    )
                )

                # remove pointless blobs
                pointers = (
                    repository.session.query(Filenode)
                    .select("id")
                    .where(Constraint("hash", "=", previous_hash))
                    .execute()
                    .fetch_all()
                )

                if not pointers:
                    logger.info(
                        format_entry(
                            "op_release",
                            f"no pointers pointing to blob '{previous_hash}'",
                        )
                    )

                    previous_blob_path = repository.blobs.joinpath(
                        previous_hash
                    )

                    if previous_blob_path.is_file():
                        logger.info(
                            format_entry(
                                "op_release",
                                f"removed blob '{previous_blob_path}'",
                            )
                        )

                        os.unlink(previous_blob_path)
            else:
                # insert new filenode
                filenode_id = (
                    repository.session.query(Filenode)
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

                logger.info(
                    format_entry(
                        "op_release", f"inserted new filenode '{filenode_id}'"
                    )
                )

                # insert new file
                file_id = (
                    repository.session.query(File)
                    .insert(
                        name=file_name,
                        directory_id=directory_id,
                        filenode_id=filenode_id,
                    )
                    .execute()
                    .get_last_row_id()
                )

                logger.info(
                    format_entry(
                        "op_release", f"inserted new file '{file_id}'"
                    )
                )

            # move temp file to blobs if not exist
            blob_path = repository.blobs.joinpath(hash)

            if not blob_path.is_file():
                # move temp file to blobs
                # if no blob exists for that hash

                logger.info(
                    format_entry(
                        "op_release",
                        f"moved file from '{resolved_path}' to '{blob_path}'",
                    )
                )

                os.rename(resolved_path, blob_path)
            else:
                # unlink temp file
                # if a blob exists for that hash

                logger.info(
                    format_entry(
                        "op_release",
                        f"removed file from '{resolved_path}' -> duplicate blob",
                    )
                )

                os.unlink(resolved_path)
