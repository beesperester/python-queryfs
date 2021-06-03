import os

from queryfs.core import Core


def op_write(core: Core, path: str, data: bytes, offset: int, fh: int) -> int:
    os.lseek(fh, offset, 0)

    return os.write(fh, data)
