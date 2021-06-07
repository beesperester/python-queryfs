import logging

from queryfs.logging import format_entry
from queryfs.repository import Repository
from queryfs.schemas import File
from queryfs.repository import unlink_file

logger = logging.getLogger("operations")


def op_unlink(repository: Repository, path: str) -> None:
    result = repository.resolve_path(path)

    logger.info(format_entry("op_unlink", path=path))

    if isinstance(result, File):
        unlink_file(repository, result)
