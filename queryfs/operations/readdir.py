from typing import Dict, Optional, Union, List, Tuple

from queryfs.core import Core
from queryfs.db import Constraint
from queryfs.schemas import File, Directory


def op_readdir(
    core: Core, path: str, fh: Optional[int] = None
) -> Union[List[str], List[Tuple[str, Dict[str, int], int]]]:
    result = core.resolve_path(path)

    dirents: List[str] = [".", ".."]

    if isinstance(result, Directory):
        file_instances = result.files(core.session)

        dirents += [x.name for x in file_instances]

        directory_instances = (
            core.session.query(Directory)
            .select()
            .where(Constraint("directory_id", "is", result.id))
            .execute()
            .fetch_all()
        )

        dirents += [x.name for x in directory_instances]
    else:
        file_instances = (
            core.session.query(File)
            .select()
            .where(Constraint("directory_id", "is", None))
            .execute()
            .fetch_all()
        )

        dirents += [x.name for x in file_instances]

        directory_instances = (
            core.session.query(Directory)
            .select()
            .where(Constraint("directory_id", "is", None))
            .execute()
            .fetch_all()
        )

        dirents += [x.name for x in directory_instances]

    return dirents