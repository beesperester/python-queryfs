import os

from queryfs.core import Core
from queryfs.db import Constraint
from queryfs.schemas import File, Filenode


def unlink_filenode(
    core: Core, filenode_instance: Filenode, recursively: bool = True
) -> None:
    previous_filenode_instance = filenode_instance.filenode(core.session)

    if previous_filenode_instance and recursively:
        # unlink previous filenode instances
        unlink_filenode(core, previous_filenode_instance, recursively)

    # store hash for later use
    hash = filenode_instance.hash

    # unlink filenode instance
    filenode_instance.delete(core.session)

    # find all pointers to hash
    # remove blob if no more pointers to blob
    pointers = (
        core.session.query(Filenode)
        .select("id")
        .where(
            Constraint("hash", "is", hash),
        )
        .execute()
        .fetch_all()
    )

    if not pointers:
        # no more pointers to blob
        blob_path = core.blobs.joinpath(hash)

        if blob_path.is_file():
            os.unlink(blob_path)


def unlink_file(core: Core, file_instance: File) -> None:
    filenode_instance = file_instance.filenode(core.session)

    if not filenode_instance:
        raise Exception(f"Missing Filenode for file '{file_instance.id}'")

    # remove file
    file_instance.delete(core.session)

    # remove related filenode
    unlink_filenode(core, filenode_instance)


def commit(core: Core, path: str) -> None:
    result = core.resolve_db_entity(path)

    if isinstance(result, File):
        filenode_instance = result.filenode(core.session)

        # new filenode instance
        if filenode_instance:
            new_filenode_instance_id = (
                core.session.query(Filenode)
                .insert(
                    hash=filenode_instance.hash,
                    ctime=filenode_instance.ctime,
                    atime=filenode_instance.atime,
                    mtime=filenode_instance.mtime,
                    size=filenode_instance.size,
                    filenode_id=filenode_instance.id,
                )
                .execute()
                .get_last_row_id()
            )

            # update file to point to new filenode
            result.update(core.session, filenode_id=new_filenode_instance_id)


def rollback(core: Core, path: str) -> None:
    result = core.resolve_db_entity(path)

    if isinstance(result, File):
        filenode_instance = result.filenode(core.session)

        if filenode_instance:
            previous_filenode_instance = filenode_instance.filenode(
                core.session
            )

            if previous_filenode_instance:
                # update file to point to previous filenode
                result.update(
                    core.session, filenode_id=previous_filenode_instance.id
                )

                # unlink headless filenode
                unlink_filenode(core, filenode_instance, False)
