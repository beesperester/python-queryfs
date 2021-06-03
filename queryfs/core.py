import os

from pathlib import Path
from typing import Optional, Union, List

from queryfs.db.session import Constraint, Session
from queryfs.models.file import File
from queryfs.models.directory import Directory
from queryfs.hashing import hash_from_bytes


def remove_empty_directories(path: Path) -> None:
    for dirent in os.listdir(path):
        dirent_path = path.joinpath(dirent)

        if dirent_path.is_dir():
            remove_empty_directories(dirent_path)
        else:
            return

    os.rmdir(path)


class Core:
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