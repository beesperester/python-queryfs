import os

from typing import Dict, Any, Optional, Union, List, Tuple
from pathlib import Path

from queryfs.core import Core
from queryfs.db.session import Constraint, Session
from queryfs.models.file import File
from queryfs.models.directory import Directory


def op_fsync(core: Core, path: str, datasync: int, fh: int) -> None:
    return os.fsync(fh)
