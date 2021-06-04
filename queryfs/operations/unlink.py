import logging

from queryfs.logging import format_entry
from queryfs.core import Core
from queryfs.schemas import File
from queryfs.repository import unlink_file

logger = logging.getLogger("operations")


def op_unlink(core: Core, path: str) -> None:
    result = core.resolve_path(path)

    logger.info(format_entry("op_unlink", path=path))

    if isinstance(result, File):
        unlink_file(core, result)
