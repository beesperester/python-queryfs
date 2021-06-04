import logging
import os

from fuse import Operations, LoggingMixIn
from pathlib import Path
from typing import Callable, Optional, Dict, Union, List, Tuple, Any

from queryfs import operations
from queryfs.core import Core


def cache(
    cache: Dict[str, Any], path: str, resolver: Callable[[], Any]
) -> Any:
    if path in cache:
        return cache[path]

    resolved = resolver()

    cache[path] = resolved

    return resolved


def invalidate(cache: Dict[str, Any], path: str) -> None:
    if path in cache:
        del cache[path]


class QueryFS(LoggingMixIn, Operations):
    def __init__(self, repository_path: Path) -> None:
        self.core = Core(repository_path)

        self.getattr_cache: Dict[str, Dict[str, int]] = {}
        self.readdir_cache: Dict[str, List[str]] = {}
        self.statfs_cache: Dict[str, Dict[str, int]] = {}

        # logs_path = repository_path.joinpath("logs.txt")

        # logging_handler = logging.FileHandler(logs_path)

        # db_logger = logging.getLogger("db")
        # operations_logger = logging.getLogger("operations")

        # db_logger.addHandler(logging_handler)
        # operations_logger.addHandler(logging_handler)

    # file system methods

    def access(self, path: str, amode: int) -> None:
        return operations.op_access(self.core, path, amode)

    def getattr(self, path: str, fh: Optional[int] = None) -> Dict[str, int]:
        return cache(
            self.getattr_cache,
            path,
            lambda: operations.op_getattr(self.core, path, fh),
        )

    def readdir(
        self, path: str, fh: int
    ) -> Union[List[str], List[Tuple[str, Dict[str, int], int]]]:
        return cache(
            self.readdir_cache,
            path,
            lambda: operations.op_readdir(self.core, path, fh),
        )

    def mkdir(self, path: str, mode: int) -> None:
        invalidate(self.readdir_cache, os.path.dirname(path))

        return operations.op_mkdir(self.core, path, mode)

    def rmdir(self, path: str) -> None:
        invalidate(self.readdir_cache, os.path.dirname(path))

        return operations.op_rmdir(self.core, path)

    def statfs(self, path: str) -> Dict[str, int]:
        return cache(
            self.statfs_cache,
            path,
            lambda: operations.op_statfs(self.core, path),
        )

    def rename(self, old: str, new: str) -> None:
        invalidate(self.readdir_cache, os.path.dirname(old))
        invalidate(self.readdir_cache, os.path.dirname(new))
        invalidate(self.getattr_cache, old)
        invalidate(self.getattr_cache, new)

        return operations.op_rename(self.core, old, new)

    chmod = None  # type: ignore

    chown = None  # type: ignore

    getxattr = None  # type: ignore

    setxattr = None  # type: ignore

    readlink = None  # type: ignore

    mknod = None  # type: ignore

    symlink = None  # type: ignore

    link = None  # type: ignore

    # file methods

    def open(self, path: str, flags: int) -> int:
        invalidate(self.readdir_cache, os.path.dirname(path))

        return operations.op_open(self.core, path, flags)

    def create(self, path: str, mode: int, fi: Optional[bool] = None) -> int:
        invalidate(self.readdir_cache, os.path.dirname(path))
        invalidate(self.getattr_cache, path)

        return operations.op_create(self.core, path, mode, fi)

    def unlink(self, path: str) -> None:
        invalidate(self.readdir_cache, os.path.dirname(path))
        invalidate(self.getattr_cache, path)

        return operations.op_unlink(self.core, path)

    utimens = None  # type: ignore

    def truncate(
        self, path: str, length: int, fh: Optional[int] = None
    ) -> None:
        invalidate(self.readdir_cache, os.path.dirname(path))
        invalidate(self.getattr_cache, path)

        return operations.op_truncate(self.core, path, length, fh)

    def read(self, path: str, size: int, offset: int, fh: int) -> bytes:
        return operations.op_read(self.core, path, size, offset, fh)

    def write(self, path: str, data: bytes, offset: int, fh: int) -> int:
        invalidate(self.getattr_cache, path)

        return operations.op_write(self.core, path, data, offset, fh)

    def flush(self, path: str, fh: int) -> None:
        invalidate(self.getattr_cache, path)

        return operations.op_flush(self.core, path, fh)

    def fsync(self, path: str, datasync: int, fh: int) -> None:
        invalidate(self.getattr_cache, path)

        return operations.op_fsync(self.core, path, datasync, fh)

    def release(self, path: str, fh: int) -> None:
        invalidate(self.readdir_cache, os.path.dirname(path))
        invalidate(self.getattr_cache, path)

        return operations.op_release(self.core, path, fh)
