from queryfs.models.file import File
import os

from queryfs.core import Core
from queryfs.db.session import Constraint


def unlink(core: Core, file_instance: File) -> None:
    hash = file_instance.hash

    core.session.query(File).delete().where(
        Constraint("id", "is", file_instance.id)
    ).execute().close()

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
