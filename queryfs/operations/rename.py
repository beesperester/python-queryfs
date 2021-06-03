import os

from typing import Dict, Any, Optional, Union, List, Tuple
from pathlib import Path

from queryfs.core import Core
from queryfs.db.session import Constraint, Session
from queryfs.models.file import File
from queryfs.models.directory import Directory


def op_rename(core: Core, old: str, new: str) -> None:
    new_name = os.path.basename(new)
    parent_directory_id = None

    old_result = core.resolve_db_entity(old)
    new_parent_result = core.resolve_db_entity(os.path.dirname(new))

    if isinstance(new_parent_result, Directory):
        parent_directory_id = new_parent_result.id

    if isinstance(old_result, File):
        core.session.query(File).update(
            name=new_name, directory_id=parent_directory_id
        ).where(Constraint("id", "is", old_result.id)).execute().close()
    elif isinstance(old_result, Directory):
        core.session.query(Directory).update(
            name=new_name, directory_id=parent_directory_id
        ).where(Constraint("id", "is", old_result.id)).execute().close()