#!/usr/bin/env python

from __future__ import with_statement

import logging
import os
import sys
import errno
import json

from shutil import copyfile
from time import time
from pathlib import Path
from typing import Any, Callable, Optional, Dict, Union, Tuple, List
from queryfs import db, PathLike
from queryfs.db.session import Constraint, Session
from queryfs.models.file import File
from queryfs.models.directory import Directory
from queryfs.hashing import hash_from_bytes, hash_from_file
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn


def format_kwargs(**kwargs: Any) -> str:
    return " ".join([f"{key}='{value}'" for key, value in kwargs.items()])


def format_lifecycle_step(name: str, **kwargs: Any) -> str:
    return f"{name}: {format_kwargs(**kwargs)}"


class Passthrough(LoggingMixIn, Operations):
    def __init__(self, repository: PathLike):
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

        # keep track of file lifecycle open / create -> read / write -> release
        self.file_lifecycles: Dict[str, List[str]] = {}

    def insert_file_lifecycle(self, file_name: str) -> bool:
        self.file_lifecycles.setdefault(file_name, [])

        return file_name in self.file_lifecycles

    def append_to_file_lifecycle(
        self, file_name: str, lifecycle_name: str, **kwargs: Any
    ) -> None:
        if file_name in self.file_lifecycles:
            lifecycle_steps = self.file_lifecycles.setdefault(file_name, [])

            lifecycle_steps.append(
                format_lifecycle_step(lifecycle_name, **kwargs)
            )

    def resolve_db_entity(
        self, path: PathLike, directory: Optional[Directory] = None
    ) -> Optional[Union[File, Directory]]:
        parts = list(filter(bool, str(path).split("/")))

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
        self, path: PathLike, directory: Optional[Directory] = None
    ) -> Union[File, Directory, PathLike]:
        parts = list(filter(bool, str(path).split("/")))

        temp_path = self.temp.joinpath("/".join(parts))

        if temp_path.exists():
            return temp_path

        db_entity = self.resolve_db_entity(path)

        if db_entity:
            return db_entity

        return temp_path

    # def rewrite_path(self, path: PathLike) -> PathLike:
    #     path = self.temp.joinpath(str(path)[1:])

    #     if path.exists():
    #         return path
    #     else:
    #         # blob path
    #         file_name = os.path.basename(path)

    #         file_instance = (
    #             self.session.query(File)
    #             .select()
    #             .where(Constraint("name", "=", "file_name"))
    #             .execute()
    #             .fetch_one()
    #         )

    #         if file_instance:
    #             blob_path = self.blobs.joinpath(file_instance.hash)

    #             if blob_path.is_file():
    #                 path = blob_path

    #     return path

    # Filesystem methods
    # ==================

    def access(self, path: PathLike, amode: int) -> None:
        result = self.resolve_path(path)

        if isinstance(result, File):
            path = self.blobs.joinpath(result.hash)
        elif isinstance(result, Directory):
            return
        else:
            path = result

        # path = self.rewrite_path(path)

        if not os.access(path, amode):
            raise FuseOSError(errno.EACCES)

    chmod = None  # type: ignore
    # def chmod(self, path, mode):
    #     full_path = self._full_path(path)
    #     return os.chmod(full_path, mode)

    chown = None  # type: ignore
    # def chown(self, path, uid, gid):
    #     full_path = self._full_path(path)
    #     return os.chown(full_path, uid, gid)

    def getattr(
        self, path: PathLike, fh: Optional[int] = None
    ) -> Dict[str, Any]:
        # original_path = path
        # basename = os.path.basename(original_path)
        result = self.resolve_path(path)

        if isinstance(result, File):
            path = self.blobs.joinpath(result.hash)
        elif isinstance(result, Directory):
            path = self.temp
        else:
            path = result

        key_names = [
            "st_atime",
            "st_ctime",
            "st_birthtime",
            "st_gid",
            "st_mode",
            "st_mtime",
            "st_nlink",
            "st_size",
            "st_uid",
        ]

        st = os.lstat(path)

        attributes = {key: getattr(st, key) for key in key_names}

        if isinstance(result, File):
            stat_db: Dict[str, Union[int, float]] = {
                "st_atime": result.atime,
                "st_birthtime": result.ctime,
                "st_ctime": result.ctime,
                "st_mtime": result.mtime,
                "st_size": result.size,
            }

            attributes = {**attributes, **stat_db}

        return attributes

    getxattr = None  # type: ignore

    def readdir(
        self, path: PathLike, fh: Optional[int] = None
    ) -> Union[List[str], List[Tuple[str, Dict[str, int], int]]]:
        result = self.resolve_path(path)

        dirents: List[str] = [".", ".."]

        if isinstance(result, Directory):
            file_instances = (
                self.session.query(File)
                .select()
                .where(Constraint("directory_id", "is", result.id))
                .execute()
                .fetch_all()
            )

            dirents += [x.name for x in file_instances]

            directory_instances = (
                self.session.query(Directory)
                .select()
                .where(Constraint("directory_id", "is", result.id))
                .execute()
                .fetch_all()
            )

            dirents += [x.name for x in directory_instances]
        else:
            file_instances = (
                self.session.query(File)
                .select()
                .where(Constraint("directory_id", "is", None))
                .execute()
                .fetch_all()
            )

            dirents += [x.name for x in file_instances]

            directory_instances = (
                self.session.query(Directory)
                .select()
                .where(Constraint("directory_id", "is", None))
                .execute()
                .fetch_all()
            )

            dirents += [x.name for x in directory_instances]

        return dirents

    readlink = None  # type: ignore
    # def readlink(self, path):
    #     pathname = os.readlink(self._full_path(path))
    #     if pathname.startswith("/"):
    #         # Path name is absolute, sanitize it.
    #         return os.path.relpath(pathname, self.root)
    #     else:
    #         return pathname

    mknod = None  # type: ignore
    # def mknod(self, path, mode, dev):
    #     return os.mknod(self._full_path(path), mode, dev)

    rmdir = None  # type: ignore
    # def rmdir(self, path):
    #     full_path = self._full_path(path)
    #     return os.rmdir(full_path)

    # mkdir = None  # type: ignore
    def mkdir(self, path: PathLike, mode: int) -> None:
        directory_name = os.path.basename(path)
        result = self.resolve_path(os.path.dirname(path))
        parent_directory_id = None

        if isinstance(result, Directory):
            parent_directory_id = result.id

        self.session.query(Directory).insert(
            name=directory_name, directory_id=parent_directory_id
        ).execute().close()

    def statfs(self, path: PathLike) -> Dict[str, Any]:
        result = self.resolve_path(path)

        if isinstance(result, File):
            path = self.blobs.joinpath(result.hash)
        elif isinstance(result, Directory):
            path = self.temp
        else:
            path = result

        key_names = [
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
        ]

        stv = os.statvfs(path)

        result = {key: getattr(stv, key) for key in key_names}

        return result

    unlink = None  # type: ignore
    # def unlink(self, path):
    #     return os.unlink(self._full_path(path))

    symlink = None  # type: ignore
    # def symlink(self, name, target):
    #     return os.symlink(name, self._full_path(target))

    def rename(self, old: PathLike, new: PathLike) -> None:
        new_name = os.path.basename(new)
        parent_directory_id = None

        old_result = self.resolve_db_entity(old)
        new_parent_result = self.resolve_db_entity(os.path.dirname(new))

        if isinstance(new_parent_result, Directory):
            parent_directory_id = new_parent_result.id

        if isinstance(old_result, File):
            self.session.query(File).update(
                name=new_name, directory_id=parent_directory_id
            ).where(Constraint("id", "is", old_result.id)).execute().close()
        elif isinstance(old_result, Directory):
            self.session.query(Directory).update(
                name=new_name, directory_id=parent_directory_id
            ).where(Constraint("id", "is", old_result.id)).execute().close()

    link = None  # type: ignore
    # def link(self, target, name):
    #     return os.link(self._full_path(target), self._full_path(name))

    utimens = None  # type: ignore
    # def utimens(self, path, times=None):
    #     return os.utime(self._full_path(path), times)

    # File methods
    # ============

    def open(self, path: PathLike, flags: int) -> int:
        original_path = Path(str(path)[1:])
        file_name = os.path.basename(path)
        result = self.resolve_path(path)

        if isinstance(result, File):
            path = self.blobs.joinpath(result.hash)
        elif isinstance(result, Directory):
            path = self.temp
        else:
            path = result

        # track lifecycle steps
        if self.insert_file_lifecycle(file_name):
            self.append_to_file_lifecycle(
                file_name, "open", path=path, flags=flags
            )

        # try and open file from temp directory
        if str(path).startswith(str(self.temp)):
            if flags == 0:
                # readable temp file
                fh = os.open(path, flags)

                self.append_to_file_lifecycle(
                    file_name,
                    "open -> opened readable temp file",
                    path=path,
                    fh=fh,
                )

                return fh
            else:
                # writable temp file
                fh = os.open(path, flags)

                # update fh for file in db
                if fh not in self.writable_file_handles:
                    self.writable_file_handles.append(fh)

                self.append_to_file_lifecycle(
                    file_name,
                    "open -> opened writable temp file",
                    path=path,
                    fh=fh,
                )

                return fh

        # try and open file from blobs diretory
        file_instance = (
            self.session.query(File)
            .select()
            .where(Constraint("name", "=", file_name))
            .execute()
            .fetch_one()
        )

        if file_instance:
            if flags == 0:
                # readable blob file
                blob_path = self.blobs.joinpath(file_instance.hash)

                fh = os.open(blob_path, flags)

                self.append_to_file_lifecycle(
                    file_name,
                    "open -> opened readable blob file",
                    path=blob_path,
                    fh=fh,
                    access_ok=os.access(blob_path, flags),
                )

                return fh
            else:
                # new writable temp file
                temp_path = self.temp.joinpath(file_name)

                if not temp_path.parent.is_dir():
                    os.makedirs(temp_path.parent, exist_ok=True)

                flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC

                fh = os.open(temp_path, flags)

                if fh not in self.writable_file_handles:
                    self.writable_file_handles.append(fh)

                self.append_to_file_lifecycle(
                    file_name,
                    "open -> opened writable temp file",
                    path=temp_path,
                    fh=fh,
                    access_ok=os.access(temp_path, flags),
                )

                return fh
        else:
            raise FuseOSError(errno.ENOENT)

    def create(
        self, path: PathLike, mode: int, fi: Optional[bool] = None
    ) -> int:
        file_name = os.path.basename(path)
        result = self.resolve_path(path)

        if isinstance(result, File):
            path = self.blobs.joinpath(result.hash)
        elif isinstance(result, Directory):
            path = self.temp
        else:
            path = result

        flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC

        # writable temp path
        temp_path = self.temp.joinpath(file_name)

        if not temp_path.parent.is_dir():
            os.makedirs(temp_path.parent, exist_ok=True)

        # track lifecycle steps
        if self.insert_file_lifecycle(file_name):
            self.append_to_file_lifecycle(
                file_name, "create", path=path, temp_path=temp_path, mode=mode
            )

        fh = os.open(temp_path, flags, mode)

        if fh not in self.writable_file_handles:
            self.writable_file_handles.append(fh)

        return fh

    def read(self, path: PathLike, size: int, offset: int, fh: int) -> bytes:
        file_name = os.path.basename(path)
        result = self.resolve_path(path)

        if isinstance(result, File):
            path = self.blobs.joinpath(result.hash)
        elif isinstance(result, Directory):
            path = self.temp
        else:
            path = result

        # track lifecycle steps
        self.append_to_file_lifecycle(
            file_name, "read", path=path, size=size, offset=offset, fh=fh
        )

        os.lseek(fh, offset, 0)

        return os.read(fh, size)

    def write(self, path: PathLike, data: bytes, offset: int, fh: int) -> int:
        file_name = os.path.basename(path)
        result = self.resolve_path(path)

        if isinstance(result, File):
            path = self.blobs.joinpath(result.hash)
        elif isinstance(result, Directory):
            path = self.temp
        else:
            path = result

        # track lifecycle steps
        self.append_to_file_lifecycle(
            file_name, "write", path=path, offset=offset, fh=fh
        )

        os.lseek(fh, offset, 0)

        return os.write(fh, data)

    truncate = None  # type: ignore
    # def truncate(self, path, length, fh=None):
    #     full_path = self._full_path(path)
    #     with open(full_path, "r+") as f:
    #         f.truncate(length)

    def flush(self, path: PathLike, fh: int) -> None:
        file_name = os.path.basename(path)
        result = self.resolve_path(path)

        if isinstance(result, File):
            path = self.blobs.joinpath(result.hash)
        elif isinstance(result, Directory):
            path = self.temp
        else:
            path = result

        # track lifecycle steps
        self.append_to_file_lifecycle(file_name, "flush", path=path, fh=fh)

        return os.fsync(fh)

    def fsync(self, path: PathLike, datasync: int, fh: int) -> None:
        file_name = os.path.basename(path)
        result = self.resolve_path(path)

        if isinstance(result, File):
            path = self.blobs.joinpath(result.hash)
        elif isinstance(result, Directory):
            path = self.temp
        else:
            path = result

        # track lifecycle steps
        self.append_to_file_lifecycle(
            file_name, "fsync", path=path, datasync=datasync, fh=fh
        )

        return os.fsync(fh)

    def release(self, path: PathLike, fh: int) -> None:
        original_path = Path(str(path)[1:])
        file_name = os.path.basename(path)
        result = self.resolve_path(path)

        if isinstance(result, File):
            path = self.blobs.joinpath(result.hash)
        elif isinstance(result, Directory):
            path = self.temp
        else:
            path = result

        # track lifecycle steps
        self.append_to_file_lifecycle(file_name, "release", path=path, fh=fh)

        os.close(fh)

        # print(json.dumps(self.file_lifecycles, indent=2))

        if fh in self.writable_file_handles:
            # remove file handle from list of writable file handles
            self.writable_file_handles.remove(fh)

            # create hash from file
            hash = hash_from_file(path)

            self.append_to_file_lifecycle(
                file_name,
                "release -> created hash",
                hash=hash,
                not_empty=hash != self.empty_hash,
            )

            if hash != self.empty_hash:
                ctime = time()

                size = Path(path).stat().st_size

                file_instance = (
                    self.session.query(File)
                    .select()
                    .where(Constraint("name", "=", file_name))
                    .execute()
                    .fetch_one()
                )

                if file_instance:
                    # udpate existing file
                    previous_hash = file_instance.hash

                    self.session.query(File).update(
                        hash=hash, atime=ctime, mtime=ctime, size=size
                    ).where(
                        Constraint("id", "=", file_instance.id)
                    ).execute().close()

                    self.append_to_file_lifecycle(
                        file_name,
                        "release -> updated file",
                        updated_file_name=file_instance.name,
                    )

                    # remove pointless blobs
                    pointers = (
                        self.session.query(File)
                        .select("id")
                        .where(Constraint("hash", "=", previous_hash))
                        .execute()
                        .fetch_all()
                    )

                    if not pointers:
                        previous_blob_path = self.blobs.joinpath(previous_hash)

                        if previous_blob_path.is_file():
                            os.unlink(previous_blob_path)
                else:
                    directory_id = None
                    parent_directory_instance = self.resolve_db_entity(
                        original_path.parent
                    )

                    if parent_directory_instance:
                        directory_id = parent_directory_instance.id

                    # insert new file
                    self.session.query(File).insert(
                        name=file_name,
                        hash=hash,
                        ctime=ctime,
                        atime=ctime,
                        mtime=ctime,
                        size=size,
                        directory_id=directory_id,
                    ).execute().close()

                    self.append_to_file_lifecycle(
                        file_name,
                        "release -> inserted file",
                        new_file_name=file_name,
                    )

                # move temp file to blobs if not exist
                blob_path = self.blobs.joinpath(hash)

                if not blob_path.is_file():
                    # move temp file to blobs
                    # if no blob exists for that hash
                    os.rename(path, blob_path)
                    # os.chmod(blob_path, 0o777)
                    # copyfile(path, blob_path)

                    self.append_to_file_lifecycle(
                        file_name,
                        "release -> moved blob file",
                        path=path,
                        blob_path=blob_path,
                    )
                else:
                    # unlink temp file
                    # if a blob exists for that hash
                    os.unlink(path)

                    self.append_to_file_lifecycle(
                        file_name, "release -> unlinked file", path=path
                    )

        if file_name in self.file_lifecycles:
            lifecycle_steps = self.file_lifecycles.setdefault(file_name, [])

            print("=" * 80)
            print(f"lifecycle complete for: {file_name}")
            print("=" * 80)
            print("\n".join(lifecycle_steps))

            del self.file_lifecycles[file_name]
