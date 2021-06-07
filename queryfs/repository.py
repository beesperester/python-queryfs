import os


from pathlib import Path
from typing import Optional, Union, List

from queryfs.db import Constraint, Session
from queryfs.schemas import File, Directory, Filenode
from queryfs.hashing import hash_from_bytes


def remove_empty_directories(path: Path) -> None:
    for dirent in os.listdir(path):
        dirent_path = path.joinpath(dirent)

        if dirent_path.is_dir():
            remove_empty_directories(dirent_path)
        else:
            return

    os.rmdir(path)


class Repository:
    def __init__(self, repository: Path):
        self.repository = Path(repository)
        self.db_name = self.repository.joinpath("queryfs.db")
        self.temp = self.repository.joinpath("temp")
        self.blobs = self.repository.joinpath("blobs")

        # create directories
        directories = [self.temp, self.blobs]

        for directory in directories:
            if not directory.is_dir():
                os.makedirs(directory, 0o777, exist_ok=True)

        # create empty blob file
        self.empty_hash = hash_from_bytes(b"")

        # with open(self.blobs.joinpath(self.empty_hash), "wb") as f:
        #     f.write(b"")

        # create tables
        self.session = Session(self.db_name)

        self.session.create_table(Directory)
        self.session.create_table(File)
        self.session.create_table(Filenode)

        # keep track of writable file handles
        self.writable_file_handles: List[int] = []

        # maintentance
        for dirent in os.listdir(self.temp):
            dirent_path = self.temp.joinpath(dirent)

            if dirent_path.is_dir():
                remove_empty_directories(dirent_path)

    def resolve_db_entity(
        self, path: str, directory: Optional[Directory] = None
    ) -> Optional[Union[File, Directory]]:
        parts = list(filter(bool, path.split("/")))

        if not parts:
            return

        # get first path element
        first_link = parts[0]

        # build constraints
        directory_id = None

        if directory:
            # add directory id constraint
            # if directory is not none
            directory_id = directory.id

        constraints: List[Constraint] = [
            Constraint("name", "is", first_link),
            Constraint("directory_id", "is", directory_id),
        ]

        directory_instance = (
            self.session.query(Directory)
            .select()
            .where(*constraints)
            .execute()
            .fetch_one()
        )

        if directory_instance:
            if len(parts) > 1:
                return self.resolve_db_entity(
                    "/".join(parts[1:]), directory_instance
                )

            return directory_instance

        file_instance = (
            self.session.query(File)
            .select()
            .where(*constraints)
            .execute()
            .fetch_one()
        )

        if file_instance:
            return file_instance

    def resolve_path(
        self, path: str, directory: Optional[Directory] = None
    ) -> Union[File, Directory, Path]:
        parts = list(filter(bool, str(path).split("/")))

        temp_path = self.temp.joinpath("/".join(parts))

        if temp_path.is_file():
            return temp_path

        if str(temp_path) == str(self.temp):
            return temp_path

        db_entity = self.resolve_db_entity(path)

        if db_entity:
            return db_entity

        return temp_path


def unlink_filenode(
    repository: Repository,
    filenode_instance: Filenode,
    recursively: bool = True,
) -> None:
    previous_filenode_instance = filenode_instance.filenode(repository.session)

    if previous_filenode_instance and recursively:
        # unlink previous filenode instances
        unlink_filenode(repository, previous_filenode_instance, recursively)

    # store hash for later use
    hash = filenode_instance.hash

    # unlink filenode instance
    filenode_instance.delete(repository.session)

    # find all pointers to hash
    # remove blob if no more pointers to blob
    pointers = (
        repository.session.query(Filenode)
        .select("id")
        .where(
            Constraint("hash", "is", hash),
        )
        .execute()
        .fetch_all()
    )

    if not pointers:
        # no more pointers to blob
        blob_path = repository.blobs.joinpath(hash)

        if blob_path.is_file():
            os.unlink(blob_path)


def unlink_file(repository: Repository, file_instance: File) -> None:
    filenode_instance = file_instance.filenode(repository.session)

    if not filenode_instance:
        raise Exception(f"Missing Filenode for file '{file_instance.id}'")

    # remove file
    file_instance.delete(repository.session)

    # remove related filenode
    unlink_filenode(repository, filenode_instance)


def commit(repository: Repository, path: str) -> None:
    result = repository.resolve_db_entity(path)

    if isinstance(result, File):
        filenode_instance = result.filenode(repository.session)

        # new filenode instance
        if filenode_instance:
            new_filenode_instance_id = (
                repository.session.query(Filenode)
                .insert(
                    hash=filenode_instance.hash,
                    ctime=filenode_instance.ctime,
                    atime=filenode_instance.atime,
                    mtime=filenode_instance.mtime,
                    size=filenode_instance.size,
                    filenode_id=filenode_instance.id,
                )
                .execute()
                .get_last_row_id()
            )

            # update file to point to new filenode
            result.update(
                repository.session, filenode_id=new_filenode_instance_id
            )


def rollback(repository: Repository, path: str) -> None:
    result = repository.resolve_db_entity(path)

    if isinstance(result, File):
        filenode_instance = result.filenode(repository.session)

        if filenode_instance:
            previous_filenode_instance = filenode_instance.filenode(
                repository.session
            )

            if previous_filenode_instance:
                # update file to point to previous filenode
                result.update(
                    repository.session,
                    filenode_id=previous_filenode_instance.id,
                )

                # unlink headless filenode
                unlink_filenode(repository, filenode_instance, False)
