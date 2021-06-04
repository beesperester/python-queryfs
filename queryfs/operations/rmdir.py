from queryfs.db.session import Constraint
from queryfs.schemas import File, Directory
from queryfs.core import Core
from queryfs.operations.unlink import unlink


def rmdir_recursively(core: Core, directory_instance: Directory) -> None:
    # get all directories
    child_directory_instances = (
        core.session.query(Directory)
        .select()
        .where(Constraint("directory_id", "is", directory_instance.id))
        .execute()
        .fetch_all()
    )

    for child_directory_instance in child_directory_instances:
        rmdir_recursively(core, child_directory_instance)

    # get all files
    file_instances = (
        core.session.query(File)
        .select()
        .where(Constraint("directory_id", "is", directory_instance.id))
        .execute()
        .fetch_all()
    )

    for file_instance in file_instances:
        unlink(core, file_instance)

    # after removing the directory's contents
    # remove the directory itself
    core.session.query(Directory).delete().where(
        Constraint("id", "is", directory_instance.id)
    ).execute().close()


def op_rmdir(core: Core, path: str) -> None:
    result = core.resolve_path(path)

    if isinstance(result, Directory):
        rmdir_recursively(core, result)