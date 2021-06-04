import os
import logging

from queryfs.logging import format_entry
from queryfs.core import Core
from queryfs.schemas import Directory

logger = logging.getLogger("operations")


def op_mkdir(core: Core, path: str, mode: int) -> None:
    directory_name = os.path.basename(path)
    result = core.resolve_path(os.path.dirname(path))
    parent_directory_id = None

    logger.info(
        format_entry("op_mkdir", path=path, mode=mode, resolved=result)
    )

    if isinstance(result, Directory):
        parent_directory_id = result.id

    core.session.query(Directory).insert(
        name=directory_name, directory_id=parent_directory_id
    ).execute().close()
