import os

from typing import Dict, Any, Optional, Union
from pathlib import Path

from queryfs.core import Core
from queryfs.db.session import Constraint, Session
from queryfs.models.file import File
from queryfs.models.directory import Directory


def op_getattr(
    core: Core, path: str, fh: Optional[int] = None
) -> Dict[str, Any]:
    # original_path = path
    # basename = os.path.basename(original_path)
    result = core.resolve_path(path)

    if isinstance(result, File):
        resolved_path = core.blobs.joinpath(result.hash)
    elif isinstance(result, Directory):
        resolved_path = core.temp
    else:
        resolved_path = result

    key_names = [
        "st_atime",
        "st_ctime",
        "st_birthtime",
        "st_gid",
        "st_mode",
        "st_mtime",
        "st_nlink",
        "st_size",
        "st_uid",
    ]

    st = os.lstat(resolved_path)

    attributes = {key: getattr(st, key) for key in key_names}

    if isinstance(result, File):
        stat_db: Dict[str, Union[int, float]] = {
            "st_atime": result.atime,
            "st_birthtime": result.ctime,
            "st_ctime": result.ctime,
            "st_mtime": result.mtime,
            "st_size": result.size,
        }

        attributes = {**attributes, **stat_db}

    return attributes
