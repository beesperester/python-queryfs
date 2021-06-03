import os

from queryfs.core import Core


def op_flush(core: Core, path: str, fh: int) -> None:
    return os.fsync(fh)
