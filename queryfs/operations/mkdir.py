import os

from typing import Dict, Any, Optional, Union, List, Tuple
from pathlib import Path

from queryfs.core import Core
from queryfs.db.session import Constraint, Session
from queryfs.models.file import File
from queryfs.models.directory import Directory


def op_mkdir(core: Core, path: str, mode: int) -> None:
    directory_name = os.path.basename(path)
    result = core.resolve_path(os.path.dirname(path))
    parent_directory_id = None

    if isinstance(result, Directory):
        parent_directory_id = result.id

    core.session.query(Directory).insert(
        name=directory_name, directory_id=parent_directory_id
    ).execute().close()
