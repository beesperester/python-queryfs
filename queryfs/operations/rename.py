import os
import logging

from queryfs.logging import format_entry
from queryfs.repository import Repository
from queryfs.schemas import File, Directory

logger = logging.getLogger("operations")


def op_rename(repository: Repository, old: str, new: str) -> None:
    new_name = os.path.basename(new)
    parent_directory_id = None

    old_result = repository.resolve_db_entity(old)
    new_parent_result = repository.resolve_db_entity(os.path.dirname(new))

    logger.info(
        format_entry("op_rename", old=old, new=new, resolved=old_result)
    )

    if isinstance(new_parent_result, Directory):
        parent_directory_id = new_parent_result.id

    if isinstance(old_result, (File, Directory)):
        old_result.update(
            repository.session, name=new_name, directory_id=parent_directory_id
        )
