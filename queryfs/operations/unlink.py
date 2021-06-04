import os
from queryfs.models.filenode import Filenode

from queryfs.core import Core
from queryfs.db.session import Constraint
from queryfs.models.file import File


def unlink(core: Core, file_instance: File) -> None:
    filenode_instance = file_instance.filenode(core.session)

    if not filenode_instance:
        raise Exception(f"Missing Filenode for file '{file_instance.id}'")

    hash = filenode_instance.hash

    core.session.query(File).delete().where(
        Constraint("id", "is", file_instance.id)
    ).execute().close()

    # remove related file node
    core.session.query(Filenode).delete().where(
        Constraint("id", "is", filenode_instance.id)
    )

    # find all pointers to hash
    # remove blob if no more pointers to blob
    pointers = (
        core.session.query(File)
        .select()
        .where(Constraint("hash", "is", hash))
        .execute()
        .fetch_all()
    )

    if not pointers:
        blob_path = core.blobs.joinpath(hash)

        os.unlink(blob_path)


def op_unlink(core: Core, path: str) -> None:
    result = core.resolve_path(path)

    if isinstance(result, File):
        unlink(core, result)