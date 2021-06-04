import os
import logging

from queryfs.logging import format_entry
from queryfs.core import Core
from queryfs.db import Constraint
from queryfs.schemas import File, Filenode

logger = logging.getLogger("operations")


def unlink(core: Core, file_instance: File) -> None:
    filenode_instance = file_instance.filenode(core.session)

    if not filenode_instance:
        raise Exception(f"Missing Filenode for file '{file_instance.id}'")

    hash = filenode_instance.hash

    # remove file
    file_instance.delete(core.session)

    # remove related filenode
    filenode_instance.delete(core.session)

    # find all pointers to hash
    # remove blob if no more pointers to blob
    pointers = (
        core.session.query(Filenode)
        .select()
        .where(
            Constraint("hash", "is", hash),
        )
        .execute()
        .fetch_all()
    )

    if not pointers:
        blob_path = core.blobs.joinpath(hash)

        os.unlink(blob_path)


def op_unlink(core: Core, path: str) -> None:
    result = core.resolve_path(path)

    logger.info(format_entry("op_unlink", path=path))

    if isinstance(result, File):
        unlink(core, result)
