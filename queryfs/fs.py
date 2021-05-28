#!/usr/bin/env python
from __future__ import print_function, absolute_import, division

import logging
import os

from collections import UserList
from hashlib import sha256
from errno import EACCES
from os.path import realpath
from statistics import mode
from threading import Lock

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from fuse import FUSE, FuseOSError, Operations


class FileHandle:
    def __init__(self, fh: int, name: str, flags: int) -> None:
        self.fh = fh
        self.name = name
        self.flags = flags

    @property
    def is_readable(self) -> bool:
        return self.flags == 0

    @property
    def is_writable(self) -> bool:
        return self.flags > 0

    @property
    def is_readable_writable(self) -> bool:
        return self.flags > 1

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} fh='{self.fh}' name='{self.name}' flags='{self.flags}' at {hex(id(self))}>"


class File:
    STATE_CLOSED = 0
    STATE_OPENED = 1
    STATE_READ = 2
    STATE_WRITTEN = 3
    STATE_CREATED = 4

    def __init__(self, fh: int, name: str) -> None:
        self.fh = fh
        self.name = name
        self.state = File.STATE_CLOSED

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} fh='{self.fh}' name='{self.name}' at {hex(id(self))}>"


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
    def __init__(self, root: str):
        self.root = realpath(root)
        self.rwlock = Lock()

        # self.fh: List[FileHandle] = []
        self.files_to_be_created: Files = Files()

    def __call__(self, op: str, path: str, *args: Any) -> Any:  # type: ignore
        # catch every function call and modify path parameter
        return super().__call__(op, self.root + path, *args)

    def access(self, path: str, amode: int) -> None:
        if not os.access(path, amode):
            raise FuseOSError(EACCES)

    chmod = os.chmod
    chown = os.chown

    def create(self, path: str, mode: int, fi: Optional[bool] = None) -> int:
        flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC

        fh = os.open(path, flags, mode)

        file = File(fh, os.path.basename(path))

        file.state = File.STATE_CREATED

        self.files_to_be_created.append(file)

        # file_handle = FileHandle(fh, os.path.basename(path), flags)

        # self.fh.append(file_handle)

        # print("create", file_handle)

        return fh

    def flush(self, path: str, fh: int) -> None:
        return os.fsync(fh)

    def fsync(self, path: str, datasync: int, fh: int) -> None:
        if datasync != 0 and hasattr(os, "fdatasync"):
            return os.fdatasync(fh)  # type: ignore

        return os.fsync(fh)

    def getattr(self, path: str, fh: Optional[int] = None) -> Dict[str, int]:
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

    getxattr = None  # type: ignore

    def link(self, target: str, source: str) -> None:
        return os.link(self.root + source, target)

    listxattr = None  # type: ignore
    mkdir = os.mkdir
    mknod = os.mknod

    def open(self, path: str, flags: int) -> int:
        # flags = os.O_RDWR | flags

        fh = os.open(path, flags)

        basename = os.path.basename(path)

        if basename in self.files_to_be_created:
            # update fh
            file = self.files_to_be_created[basename]

            print("update", file)

            file.state = File.STATE_OPENED

            file.fh = fh
        else:
            file = File(fh, basename)

            self.files_to_be_created.append(file)

        # file_handle = FileHandle(fh, os.path.basename(path), flags)

        # self.fh.append(file_handle)

        # print("open", file_handle)

        return fh

    def read(self, path: str, size: int, offset: int, fh: int) -> bytes:
        with self.rwlock:
            os.lseek(fh, offset, 0)
            return os.read(fh, size)

    def readdir(
        self, path: str, fh: int
    ) -> Union[List[str], List[Tuple[str, Dict[str, int], int]]]:
        return [".", ".."] + os.listdir(path)

    readlink = os.readlink

    def release(self, path: str, fh: int):
        # if fh in self.fh_open:
        #     print("release", fh)

        # print("release", fh, self.fh[fh])

        # sha256_hash = sha256()

        # os.lseek(fh, 0, 0)
        # for data in iter(lambda: os.read(fh, 1024), b""):
        #     sha256_hash.update(data)

        # print(sha256_hash.hexdigest())

        # fh_index = [x.fh for x in self.fh].index(fh)

        # file_handle = self.fh[fh_index]

        # # if file_handle.is_writable:
        # print("release", file_handle)

        os.close(fh)

        if fh in self.files_to_be_created:
            file = self.files_to_be_created[fh]

            if file.state == File.STATE_WRITTEN:
                print("release", file)

                sha256_hash = sha256()

                with open(os.path.join(self.root, file.name), "rb") as f:
                    for byte_block in iter(lambda: f.read(4096), b""):
                        sha256_hash.update(byte_block)

                print("checksum", sha256_hash.hexdigest())

                self.files_to_be_created.remove(file)
        #     else:
        #         print("do not release yet", file)
        # else:
        #     print("unable to find fh", fh)

    def rename(self, old: str, new: str) -> None:
        return os.rename(old, self.root + new)

    rmdir = os.rmdir

    def statfs(self, path: str):
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
    utimens = os.utime

    def write(self, path: str, data: bytes, offset: int, fh: int) -> int:
        print(len(data), offset, fh)

        if fh in self.files_to_be_created:
            file = self.files_to_be_created[fh]

            file.state = File.STATE_WRITTEN

        with self.rwlock:
            os.lseek(fh, offset, 0)

            return os.write(fh, data)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("root")
    parser.add_argument("mount")
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING)
    fuse = FUSE(
        Loopback(args.root), args.mount, foreground=True, allow_other=True
    )
