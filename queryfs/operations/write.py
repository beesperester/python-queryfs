import os

from typing import Dict, Any, Optional, Union, List, Tuple
from pathlib import Path

from queryfs.core import Core
from queryfs.db.session import Constraint, Session
from queryfs.models.file import File
from queryfs.models.directory import Directory


def op_write(core: Core, path: str, data: bytes, offset: int, fh: int) -> int:
    os.lseek(fh, offset, 0)

    return os.write(fh, data)
