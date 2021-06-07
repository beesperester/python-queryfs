import logging

from typing import Dict, Optional, Union, List, Tuple

from queryfs.logging import format_entry
from queryfs.repository import Repository
from queryfs.db import Constraint
from queryfs.schemas import File, Directory

logger = logging.getLogger("operations")


def op_readdir(
    repository: Repository, path: str, fh: Optional[int] = None
) -> Union[List[str], List[Tuple[str, Dict[str, int], int]]]:
    result = repository.resolve_path(path)

    logger.info(format_entry("op_readdir", path=path, fh=fh, resolved=result))

    dirents: List[str] = [".", ".."]

    if isinstance(result, Directory):
        file_instances = result.files(repository.session)

        dirents += [x.name for x in file_instances]

        directory_instances = (
            repository.session.query(Directory)
            .select()
            .where(Constraint("directory_id", "is", result.id))
            .execute()
            .fetch_all()
        )

        dirents += [x.name for x in directory_instances]
    else:
        file_instances = (
            repository.session.query(File)
            .select()
            .where(Constraint("directory_id", "is", None))
            .execute()
            .fetch_all()
        )

        dirents += [x.name for x in file_instances]

        directory_instances = (
            repository.session.query(Directory)
            .select()
            .where(Constraint("directory_id", "is", None))
            .execute()
            .fetch_all()
        )

        dirents += [x.name for x in directory_instances]

    logger.info(format_entry("op_readdir", dirents=dirents))

    return dirents