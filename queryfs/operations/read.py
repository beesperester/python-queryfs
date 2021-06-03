import os

from queryfs.core import Core


def op_read(core: Core, path: str, size: int, offset: int, fh: int) -> bytes:
    os.lseek(fh, offset, 0)

    return os.read(fh, size)
