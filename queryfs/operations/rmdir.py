import logging

from queryfs.logging import format_entry
from queryfs.db import Constraint
from queryfs.schemas import File, Directory
from queryfs.repository import Repository
from queryfs.repository import unlink_file

logger = logging.getLogger("operations")


def rmdir_recursively(
    repository: Repository, directory_instance: Directory
) -> None:
    # get all directories
    child_directory_instances = (
        repository.session.query(Directory)
        .select()
        .where(Constraint("directory_id", "is", directory_instance.id))
        .execute()
        .fetch_all()
    )

    for child_directory_instance in child_directory_instances:
        rmdir_recursively(repository, child_directory_instance)

    # get all files
    file_instances = (
        repository.session.query(File)
        .select()
        .where(Constraint("directory_id", "is", directory_instance.id))
        .execute()
        .fetch_all()
    )

    for file_instance in file_instances:
        unlink_file(repository, file_instance)

    # after removing the directory's contents
    # remove the directory itself
    directory_instance.delete(repository.session)


def op_rmdir(repository: Repository, path: str) -> None:
    result = repository.resolve_path(path)

    logger.info(format_entry("op_rmdir", path=path, resolved=result))

    if isinstance(result, Directory):
        rmdir_recursively(repository, result)
