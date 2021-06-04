import os

from queryfs.core import Core
from queryfs.schemas import Directory


def op_mkdir(core: Core, path: str, mode: int) -> None:
    directory_name = os.path.basename(path)
    result = core.resolve_path(os.path.dirname(path))
    parent_directory_id = None

    if isinstance(result, Directory):
        parent_directory_id = result.id

    core.session.query(Directory).insert(
        name=directory_name, directory_id=parent_directory_id
    ).execute().close()
