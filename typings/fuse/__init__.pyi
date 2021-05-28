"""
This type stub file was generated by pyright.
"""

import ctypes
from typing import Any, AnyStr, Dict, List, Optional, Tuple, Union
from __future__ import absolute_import, division, print_function

log: str = ...
_system: str = ...
_machine: str = ...

class c_timespec(ctypes.Structure):
    _fields_: Any = ...

class c_utimbuf(ctypes.Structure):
    _fields_: Any = ...

class c_stat(ctypes.Structure): ...

_libfuse_path: str = ...

class fuse_file_info(ctypes.Structure):
    _fields_: Any = ...

class fuse_context(ctypes.Structure):
    _fields_: Any = ...

class fuse_operations(ctypes.Structure):
    _fields_: Any = ...

def time_of_timespec(ts: Any, use_ns: Any = ...) -> Any: ...
def set_st_attrs(st: Any, attrs: Any, use_ns: Any = ...) -> Any: ...
def fuse_get_context() -> Tuple[int, int, int]:
    "Returns a (uid, gid, pid) tuple"
    ...

def fuse_exit() -> None:
    """
    This will shutdown the FUSE mount and cause the call to FUSE(...) to
    return, similar to sending SIGINT to the process.

    Flags the native FUSE session as terminated and will cause any running FUSE
    event loops to exit on the next opportunity. (see fuse.c::fuse_exit)
    """
    ...

class FuseOSError(OSError):
    def __init__(self, errno: int) -> None: ...

class FUSE(object):
    """
    This class is the lower level interface and should not be subclassed under
    normal use. Its methods are called by fuse.

    Assumes API version 2.6 or later.
    """

    def __init__(
        self,
        operations: Operations,
        mountpoint: str,
        raw_fi: Optional[bool] = ...,
        encoding: str = ...,
        **kwargs: Any
    ) -> None:
        """
        Setting raw_fi to True will cause FUSE to pass the fuse_file_info
        class as is to Operations, instead of just the fh field.

        This gives you access to direct_io, keep_cache, etc.
        """
        ...

class Operations(object):
    """
    This class should be subclassed and passed as an argument to FUSE on
    initialization. All operations should raise a FuseOSError exception on
    error.

    When in doubt of what an operation should do, check the FUSE header file
    or the corresponding system call man page.
    """

    def __call__(self, op: str, *args: Any) -> Any: ...
    def access(self, path: str, amode: int) -> None: ...
    bmap: Any = ...
    def chmod(self, path: str, mode: int) -> None: ...
    def chown(self, path: str, uid: int, gid: int) -> None: ...
    def create(self, path: str, mode: int, fi: Optional[bool] = ...) -> int:
        """
        When raw_fi is False (default case), fi is None and create should
        return a numerical file handle.

        When raw_fi is True the file handle should be set directly by create
        and return 0.
        """
        ...
    def destroy(self, path: str) -> None:
        "Called on filesystem destruction. Path is always /"
        ...
    def flush(self, path: str, fh: int) -> None: ...
    def fsync(self, path: str, datasync: int, fh: int) -> None: ...
    def fsyncdir(self, path: str, datasync: int, fh: int) -> None: ...
    def getattr(self, path: str, fh: Optional[int] = ...) -> Dict[str, int]:
        """
        Returns a dictionary with keys identical to the stat C structure of
        stat(2).

        st_atime, st_mtime and st_ctime should be floats.

        NOTE: There is an incompatibility between Linux and Mac OS X
        concerning st_nlink of directories. Mac OS X counts all files inside
        the directory, while Linux counts only the subdirectories.
        """
        ...
    def getxattr(
        self, path: str, name: str, position: int = ...
    ) -> Dict[str, int]: ...
    def init(self, path: str) -> None:
        """
        Called on filesystem initialization. (Path is always /)

        Use it instead of __init__ if you start threads on initialization.
        """
        ...
    def ioctl(
        self, path: str, cmd: str, arg: str, fip: int, flags: int, data: Any
    ) -> None: ...
    def link(self, target: str, source: str) -> None:
        "creates a hard link `target -> source` (e.g. ln source target)"
        ...
    def listxattr(self, path: str) -> Any: ...
    lock: int = ...
    def mkdir(self, path: str, mode: int) -> None: ...
    def mknod(self, path: str, mode: int, dev: int) -> None: ...
    def open(self, path: str, flags: int) -> int:
        """
        When raw_fi is False (default case), open should return a numerical
        file handle.

        When raw_fi is True the signature of open becomes:
            open(self, path, fi)

        and the file handle should be set directly.
        """
        ...
    def opendir(self, path: str) -> int:
        "Returns a numerical file handle."
        ...
    def read(self, path: str, size: int, offset: int, fh: int) -> bytes:
        "Returns a string containing the data requested."
        ...
    def readdir(
        self, path: str, fh: int
    ) -> Union[List[str], List[Tuple[str, Dict[str, int], int]]]:
        """
        Can return either a list of names, or a list of (name, attrs, offset)
        tuples. attrs is a dict as in getattr.
        """
        ...
    def readlink(self, path: str) -> AnyStr: ...
    def release(self, path: str, fh: int) -> None: ...
    def releasedir(self, path: str, fh: int) -> None: ...
    def removexattr(self, path: str, name: str) -> None: ...
    def rename(self, old: str, new: str) -> None: ...
    def rmdir(self, path: str) -> None: ...
    def setxattr(
        self,
        path: str,
        name: str,
        value: str,
        options: Any,
        position: int = ...,
    ) -> Any: ...
    def statfs(self, path: str) -> Dict[str, int]:
        """
        Returns a dictionary with keys identical to the statvfs C structure of
        statvfs(3).

        On Mac OS X f_bsize and f_frsize must be a power of 2
        (minimum 512).
        """
        ...
    def symlink(self, target: str, source: str) -> None:
        "creates a symlink `target -> source` (e.g. ln -s source target)"
        ...
    def truncate(
        self, path: str, length: int, fh: Optional[int] = ...
    ) -> None: ...
    def unlink(self, path: str) -> None: ...
    def utimens(self, path: str, times: Tuple[int, int] = ...) -> None:
        "Times is a (atime, mtime) tuple. If None use current time."
        ...
    def write(self, path: str, data: bytes, offset: int, fh: int) -> int: ...

class LoggingMixIn:
    log: Any = ...
    def __call__(self, op: str, path: str, *args: Any) -> Any: ...
