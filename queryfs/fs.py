import os
import logging

from hashlib import sha256
from typing import Any, Optional, Union, Dict, List
from pathlib import Path
from threading import Lock
from queryfs import db
from queryfs.db import file
from queryfs.db.file import File


logger = logging.getLogger("fs")


class QueryFS:
    def __init__(self, db_name: Union[str, Path]) -> None:
        self.db_name = db_name

        # create tables
        db.create_table(self.db_name, File)

    def create(
        self, path: Union[str, Path], mode: int, fi: Optional[bool] = None
    ) -> int:
        logger.info(
            format_log_entry(
                self.__class__.__name__, "create", path=path, mode=mode
            )
        )

        file_name = os.path.basename(path)
        flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC

        fh = os.open(path, flags, mode)

        db.insert(self.db_name, File, name=file_name, fh=fh)

        return fh

    def open(self, path: Union[str, Path], flags: int) -> int:
        logger.info(
            format_log_entry(
                self.__class__.__name__, "open", path=path, flags=flags
            )
        )

        file_name = os.path.basename(path)

        fh = os.open(path, flags)

        file_instance = file.fetch_one_by_name(self.db_name, file_name)

        if flags > 0:
            file.update_fh(self.db_name, file_instance, fh)

        return fh

    def read(
        self, path: Union[str, Path], size: int, offset: int, fh: int
    ) -> bytes:
        logger.info(
            format_log_entry(
                self.__class__.__name__,
                "read",
                path=path,
                size=size,
                offset=offset,
                fh=fh,
            )
        )

        with Lock():
            os.lseek(fh, offset, 0)

            return os.read(fh, size)

    def write(
        self, path: Union[str, Path], data: bytes, offset: int, fh: int
    ) -> int:
        logger.info(
            format_log_entry(
                self.__class__.__name__,
                "write",
                path=path,
                offset=offset,
                fh=fh,
            )
        )

        with Lock():
            os.lseek(fh, offset, 0)

            return os.write(fh, data)

    def release(self, path: Union[str, Path], fh: int) -> None:
        logger.info(
            format_log_entry(
                self.__class__.__name__,
                "release",
                path=path,
                fh=fh,
            )
        )

        os.close(fh)

        try:
            file_instance = file.fetch_one_by_fh(self.db_name, fh)

            base_path = Path("/Users/bernhardesperester/git/python-queryfs")

            file_path = base_path.joinpath(file_instance.name)

            sha256_hash = sha256()

            with open(file_path, "rb") as f:
                for byte_block in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(byte_block)

            hash = sha256_hash.hexdigest()

            file.update_hash(self.db_name, file_instance, hash)

            file.update_fh(self.db_name, file_instance, 0)

            logger.info(
                format_log_entry(
                    self.__class__.__name__,
                    "release -> hash file",
                    hash=hash,
                )
            )
        except db.NotFoundException:
            pass


def format_log_entry(*args: Any, **kwargs: Any) -> str:
    result = ""

    if args:
        result = "\t".join(args)

    if kwargs:
        result += (
            ":"
            + "\t"
            + " ".join([f"{key}='{value}'" for key, value in kwargs.items()])
        )

    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger("db").setLevel(logging.WARNING)
    logging.getLogger("dbfs").setLevel(logging.WARNING)

    base_path = Path("/Users/bernhardesperester/git/python-queryfs")

    db_path = base_path.joinpath("test.db")

    if db_path.is_file():
        os.unlink(db_path)

    test_file_path = base_path.joinpath("test.txt")

    if test_file_path.is_file():
        os.unlink(test_file_path)

    query_fs = QueryFS(db_path)

    fh = query_fs.create(test_file_path, mode=0o777)

    query_fs.write(test_file_path, b"Hello world", 0, fh)

    query_fs.release(test_file_path, fh)

    fh = query_fs.open(test_file_path, os.O_RDONLY)

    data = query_fs.read(test_file_path, 1024, 0, fh)

    print(data)

    query_fs.release(test_file_path, fh)

    fh = query_fs.open(test_file_path, os.O_WRONLY)

    query_fs.write(test_file_path, b"Bli bla blub", 0, fh)

    query_fs.release(test_file_path, fh)
