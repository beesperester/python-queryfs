import os
import logging

from typing import AnyStr, Optional, Any, Dict, Tuple
from pathlib import Path
from threading import Lock
from queryfs import db, PathLike
from queryfs.db import file
from queryfs.db.file import File
from queryfs.logging import format_log_entry
from queryfs.hashing import hash_from_file


logger = logging.getLogger("fs")


class FuseFiles(object):
    def __init__(self, repository: PathLike) -> None:
        self.repository = Path(repository)
        self.db_name = self.repository.joinpath("queryfs.db")
        self.temp = self.repository.joinpath("temp")
        self.blobs = self.repository.joinpath("blobs")

        # create directories
        directories = [self.temp, self.blobs]

        for directory in directories:
            if not directory.is_dir():
                os.makedirs(directory, exist_ok=True)

        # create tables
        db.create_table(self.db_name, File)

    def __call__(self, op: str, path: str, *args: Any) -> Any:

        return super().__call__(op, path, *args)  # type: ignore

    def getattr(self, path: str, fh: Optional[int] = None) -> Dict[str, int]:
        logger.info(
            format_log_entry(
                self.__class__.__name__, "getattr", path=path, fh=fh
            )
        )

        file_name = os.path.basename(path)

        file_instance = file.fetch_one_by_name(self.db_name, file_name)

        st = os.lstat(self.blobs.joinpath(file_instance.hash))
        return dict(
            (key, getattr(st, key))
            for key in (
                "st_atime",
                "st_ctime",
                "st_gid",
                "st_mode",
                "st_mtime",
                "st_nlink",
                "st_size",
                "st_uid",
            )
        )

    def create(
        self, path: PathLike, mode: int, fi: Optional[bool] = None
    ) -> int:
        logger.info(
            format_log_entry(
                self.__class__.__name__, "create", path=path, mode=mode
            )
        )

        file_name = os.path.basename(path)
        flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC

        fh = os.open(self.temp.joinpath(file_name), flags, mode)

        file.insert(self.db_name, name=file_name, fh=fh)

        return fh

    def open(self, path: PathLike, flags: int) -> int:
        logger.info(
            format_log_entry(
                self.__class__.__name__, "open", path=path, flags=flags
            )
        )

        file_name = os.path.basename(path)

        file_instance = file.fetch_one_by_name(self.db_name, file_name)

        if flags > 0:
            flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC

            fh = os.open(self.temp.joinpath(file_name), flags, 0o511)

            file.update_fh(self.db_name, file_instance, fh)
        else:
            fh = os.open(self.blobs.joinpath(file_instance.hash), flags)

        return fh

    def read(self, path: PathLike, size: int, offset: int, fh: int) -> bytes:
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

    def write(self, path: PathLike, data: bytes, offset: int, fh: int) -> int:
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

    def release(self, path: PathLike, fh: int) -> None:
        logger.info(
            format_log_entry(
                self.__class__.__name__,
                "release",
                path=path,
                fh=fh,
            )
        )

        os.close(fh)

        file_name = os.path.basename(path)

        try:
            file_instance = file.fetch_one_by_fh(self.db_name, fh)

            previous_hash = file_instance.hash

            temp_path = self.temp.joinpath(file_name)

            # create hash from file
            hash = hash_from_file(temp_path)

            file.update_hash(self.db_name, file_instance, hash)

            # release file handle
            file.update_fh(self.db_name, file_instance, 0)

            logger.info(
                format_log_entry(
                    self.__class__.__name__,
                    "release -> hash file",
                    hash=hash,
                )
            )

            # move temp file to blobs if not exist
            blob_path = self.blobs.joinpath(hash)

            if not blob_path.is_file():
                os.rename(temp_path, blob_path)

            # remove headless blobs
            pointers = file.fetch_many_by_hash(self.db_name, previous_hash)

            if not pointers:
                previous_blob_path = self.blobs.joinpath(previous_hash)

                if previous_blob_path.is_file():
                    os.unlink(previous_blob_path)

        except db.NotFoundException:
            pass

    def flush(self, path: PathLike, fh: int) -> None:
        logger.info(
            format_log_entry(
                self.__class__.__name__, "flush", path=path, fh=fh
            )
        )

        return os.fsync(fh)

    def fsync(self, path: PathLike, datasync: int, fh: int) -> None:
        logger.info(
            format_log_entry(
                self.__class__.__name__,
                "fsync",
                path=path,
                datasync=datasync,
                fh=fh,
            )
        )

        if datasync != 0 and hasattr(os, "fdatasync"):
            return os.fdatasync(fh)  # type: ignore

        return os.fsync(fh)

    def symlink(self, target: PathLike, source: PathLike) -> None:
        logger.info(
            format_log_entry(
                self.__class__.__name__,
                "symlink",
                target=target,
                source=source,
            )
        )

        # TODO modify File schema to support symlinks

        # target_file_name = os.path.basename(target)
        # source_file_name = os.path.basename(source)

        # source_file_instance = file.fetch_one_by_name(
        #     self.db_name, source_file_name
        # )

        # # insert new pointer to same hash
        # file.insert(
        #     self.db_name, name=target_file_name, hash=source_file_instance.hash
        # )

        raise NotImplementedError()

    def readlink(self, path: PathLike) -> AnyStr:
        logger.info(
            format_log_entry(self.__class__.__name__, "symlink", path=path)
        )

        raise NotImplementedError()

        # file_name = os.path.basename(path)

        # file_instance = file.fetch_one_by_name(self.db_name, file_name)

        # path = self.blobs.joinpath(file_instance.hash)

    def unlink(self, path: PathLike) -> None:
        logger.info(
            format_log_entry(self.__class__.__name__, "unlink", path=path)
        )

        # remove from db
        file_name = os.path.basename(path)

        file_instance = file.fetch_one_by_name(self.db_name, file_name)

        file.delete(self.db_name, file_instance)

        # remove headless blobs
        pointers = file.fetch_many_by_hash(self.db_name, file_instance.hash)

        if not pointers:
            previous_blob_path = self.blobs.joinpath(file_instance.hash)

            if previous_blob_path.is_file():
                os.unlink(previous_blob_path)

    def truncate(
        self, path: PathLike, length: int, fh: Optional[int] = None
    ) -> None:
        logger.info(
            format_log_entry(
                self.__class__.__name__, "truncate", length=length, fh=fh
            )
        )

    def utimens(self, path: PathLike, times: Tuple[int, int] = ...) -> None:
        logger.info(
            format_log_entry(
                self.__class__.__name__, "utimens", path=path, times=times
            )
        )

        file_name = os.path.basename(path)

        file_instance = file.fetch_one_by_name(self.db_name, file_name)

        file.update_utimens(self.db_name, file_instance, times)

        # path = self.blobs.joinpath(file_instance.hash)

        # return os.utime(path, times)
