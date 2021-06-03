import os

from queryfs.core import Core


def op_fsync(core: Core, path: str, datasync: int, fh: int) -> None:
    return os.fsync(fh)
