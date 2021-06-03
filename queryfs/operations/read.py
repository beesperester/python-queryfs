import os

from typing import Dict, Any, Optional, Union, List, Tuple
from pathlib import Path

from queryfs.core import Core
from queryfs.db.session import Constraint, Session
from queryfs.models.file import File
from queryfs.models.directory import Directory


def op_read(core: Core, path: str, size: int, offset: int, fh: int) -> bytes:
    os.lseek(fh, offset, 0)

    return os.read(fh, size)
