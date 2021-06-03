from fuse import Operations, LoggingMixIn
from pathlib import Path
from typing import Optional, Dict, Union, List, Tuple

from queryfs import operations
from queryfs.core import Core


class QueryFS(LoggingMixIn, Operations):
    def __init__(self, repository_path: Path) -> None:
        self.core = Core(repository_path)

    # file system methods

    def access(self, path: str, amode: int) -> None:
        return operations.op_access(self.core, path, amode)

    def getattr(self, path: str, fh: Optional[int] = None) -> Dict[str, int]:
        return operations.op_getattr(self.core, path, fh)

    def readdir(
        self, path: str, fh: int
    ) -> Union[List[str], List[Tuple[str, Dict[str, int], int]]]:
        return operations.op_readdir(self.core, path, fh)

    def mkdir(self, path: str, mode: int) -> None:
        return operations.op_mkdir(self.core, path, mode)

    rmdir = None  # type: ignore

    def statfs(self, path: str) -> Dict[str, int]:
        return operations.op_statfs(self.core, path)

    def rename(self, old: str, new: str) -> None:
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
        return operations.op_open(self.core, path, flags)

    def create(self, path: str, mode: int, fi: Optional[bool] = None) -> int:
        return operations.op_create(self.core, path, mode, fi)

    unlink = None  # type: ignore

    utimens = None  # type: ignore

    truncate = None  # type: ignore

    def read(self, path: str, size: int, offset: int, fh: int) -> bytes:
        return operations.op_read(self.core, path, size, offset, fh)

    def write(self, path: str, data: bytes, offset: int, fh: int) -> int:
        return operations.op_write(self.core, path, data, offset, fh)

    def flush(self, path: str, fh: int) -> None:
        return operations.op_flush(self.core, path, fh)

    def fsync(self, path: str, datasync: int, fh: int) -> None:
        return operations.op_fsync(self.core, path, datasync, fh)

    def release(self, path: str, fh: int) -> None:
        return operations.op_release(self.core, path, fh)
