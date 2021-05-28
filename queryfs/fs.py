#!/usr/bin/env python
from __future__ import print_function, absolute_import, division

import logging
import os
from posixpath import basename
import sqlite3

from collections import UserList
from hashlib import sha256
from errno import EACCES
from os.path import realpath
from statistics import mode
from threading import Lock

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from fuse import FUSE, FuseOSError, Operations
from queryfs import db


class File:
    LIFECYCLE_OPEN = "open"
    LIFECYCLE_CREATE = "create"
    LIFECYCLE_WRITE = "write"
    LIFECYCLE_READ = "read"

    def __init__(self, fh: int, name: str) -> None:
        self.fh = fh
        self.name = name
        self.lifecycle: List[str] = []

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} fh='{self.fh}' name='{self.name}' lifecycle='{self.lifecycle}' at {hex(id(self))}>"


class Files:
    def __init__(self) -> None:
        self.data: List[File] = []

    def __contains__(self, other: Union[int, str]) -> bool:
        if isinstance(other, int):
            return other in [x.fh for x in self.data]

        return other in [x.name for x in self.data]

    def __getitem__(self, key: Union[int, str]) -> File:
        if isinstance(key, int):
            index = [x.fh for x in self.data].index(key)
        else:
            index = [x.name for x in self.data].index(key)

        return self.data[index]

    def append(self, file: File) -> None:
        self.data.append(file)

    def remove(self, item: File) -> None:
        self.data.remove(item)


class Loopback(Operations):
    def __init__(self, root: str, db_name: str):
        self.db_name = db_name

        db.init(db_name)

        self.root = realpath(root)
        self.rwlock = Lock()

        self.blobs = Path(self.root).joinpath("blobs")
        self.temp = Path(self.root).joinpath("temp")

        if not self.blobs.is_dir():
            os.makedirs(self.blobs, mode=0o777)

        if not self.temp.is_dir():
            os.makedirs(self.temp, mode=0o777)

        # self.file_handles: Files = Files()

    def __call__(self, op: str, path: str, *args: Any) -> Any:  # type: ignore
        # catch every function call and modify path parameter
        return super().__call__(op, self.root + path, *args)

    # filesystem methods

    def access(self, path: str, amode: int) -> None:
        file_basename = os.path.basename(path)

        result = db.get_file_by_name(self.db_name, file_basename)

        if result:
            return

        if not os.access(path, amode):
            raise FuseOSError(EACCES)

    def readdir(
        self, path: str, fh: int
    ) -> Union[List[str], List[Tuple[str, Dict[str, int], int]]]:
        return [".", ".."] + [x.name for x in db.readdir(self.db_name)]

    mkdir = os.mkdir
    rmdir = os.rmdir
    mknod = os.mknod
    chmod = os.chmod
    chown = os.chown

    # file methods

    def create(self, path: str, mode: int, fi: Optional[bool] = None) -> int:
        file_basename = os.path.basename(path)
        flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC

        temp_path = self.temp.joinpath(file_basename)

        fh = os.open(temp_path, flags, mode)

        db.create(self.db_name, file_basename, fh)

        print("create", file_basename, fh)

        return fh

    def open(self, path: str, flags: int) -> int:
        file_basename = os.path.basename(path)

        file = db.get_file_by_name(self.db_name, file_basename)

        if file:
            if flags == 0:
                # read only
                path = str(self.blobs.joinpath(file.hash))
                path = str(self.temp.joinpath(file_basename))
                open(path, "wb")
                print("path to read only file", path)
            else:
                # write
                path = str(self.temp.joinpath(file_basename))

                open(path, "wb")

                print("path to empty temp file", path)
        else:
            path = str(self.temp.joinpath(file_basename))

            print("path to existing temp file", path)

        fh = os.open(path, flags)

        if file and flags > 0:
            # lock file with filehandle in DB
            db.lock(self.db_name, file.id, fh)

        print(f"open ({flags})", file_basename, fh)

        return fh

    def read(self, path: str, size: int, offset: int, fh: int) -> bytes:
        with self.rwlock:
            os.lseek(fh, offset, 0)
            return os.read(fh, size)

    def write(self, path: str, data: bytes, offset: int, fh: int) -> int:
        with self.rwlock:
            os.lseek(fh, offset, 0)

            return os.write(fh, data)

    def release(self, path: str, fh: int) -> None:
        os.close(fh)

        file = db.get_file_by_lock(self.db_name, fh)

        if file:
            print("release", file.name, fh)

        if file and file.lock > 0:
            sha256_hash = sha256()

            file_path = self.temp.joinpath(file.name)

            with open(file_path, "rb") as f:
                for byte_block in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(byte_block)

            hash = sha256_hash.hexdigest()

            # update hash
            db.release(self.db_name, file.name, hash)

            blob_file = self.blobs.joinpath(hash)

            if not blob_file.is_file():
                os.rename(file_path, blob_file)

    def flush(self, path: str, fh: int) -> None:
        return os.fsync(fh)

    def fsync(self, path: str, datasync: int, fh: int) -> None:
        if datasync != 0 and hasattr(os, "fdatasync"):
            return os.fdatasync(fh)  # type: ignore

        return os.fsync(fh)

    def getattr(self, path: str, fh: Optional[int] = None) -> Dict[str, int]:
        file_basename = os.path.basename(path)

        file = db.get_file_by_name(self.db_name, file_basename)

        if file:
            path = str(self.blobs.joinpath(file.hash))
        else:
            path = str(self.temp.joinpath(file_basename))

        st = os.lstat(path)
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

    def link(self, target: str, source: str) -> None:
        return os.link(self.root + source, target)

    def rename(self, old: str, new: str) -> None:
        return os.rename(old, self.root + new)

    def statfs(self, path: str):
        file_basename = os.path.basename(path)

        file = db.get_file_by_name(self.db_name, file_basename)

        if file:
            path = str(self.blobs.joinpath(file.hash))
        else:
            path = str(self.temp.joinpath(file_basename))

        stv = os.statvfs(path)
        return dict(
            (key, getattr(stv, key))
            for key in (
                "f_bavail",
                "f_bfree",
                "f_blocks",
                "f_bsize",
                "f_favail",
                "f_ffree",
                "f_files",
                "f_flag",
                "f_frsize",
                "f_namemax",
            )
        )

    def symlink(self, target: str, source: str):
        return os.symlink(source, target)

    def truncate(self, path: str, length: int, fh: Optional[int] = None):
        with open(path, "r+") as f:
            f.truncate(length)

    unlink = os.unlink
    readlink = os.readlink
    utimens = os.utime


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("root")
    parser.add_argument("mount")
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING)
    fuse = FUSE(
        Loopback(args.root, "files.db"),
        args.mount,
        foreground=True,
        allow_other=True,
    )
