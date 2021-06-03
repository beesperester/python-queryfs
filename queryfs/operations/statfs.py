import os

from typing import Dict, Any

from queryfs.core import Core
from queryfs.models.file import File
from queryfs.models.directory import Directory


def op_statfs(core: Core, path: str) -> Dict[str, Any]:
    result = core.resolve_path(path)

    if isinstance(result, File):
        path_resolved = core.blobs.joinpath(result.hash)
    elif isinstance(result, Directory):
        path_resolved = core.temp
    else:
        path_resolved = result

    key_names = [
        "f_bavail",
        "f_bfree",
        "f_blocks",
        "f_bsize",
        "f_favail",
        "f_ffree",
        "f_files",
        "f_flag",
        "f_frsize",
        "f_namemax",
    ]

    stv = os.statvfs(path_resolved)

    result = {key: getattr(stv, key) for key in key_names}

    return result
