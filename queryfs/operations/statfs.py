import os

from typing import Dict, Any

from queryfs.core import Core
from queryfs.schemas import File, Directory


def op_statfs(core: Core, path: str) -> Dict[str, Any]:
    result = core.resolve_path(path)

    if isinstance(result, File):
        filenode_instance = result.filenode(core.session)

        if filenode_instance:
            resolved_path = core.blobs.joinpath(filenode_instance.hash)
        else:
            raise Exception("Missing Filenode")
    elif isinstance(result, Directory):
        resolved_path = core.temp
    else:
        resolved_path = result

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

    stv = os.statvfs(resolved_path)

    result = {key: getattr(stv, key) for key in key_names}

    return result
