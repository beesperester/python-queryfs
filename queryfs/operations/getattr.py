import os
from typing import Dict, Any, Optional, Union

from queryfs.core import Core
from queryfs.schemas import File, Directory, Filenode


def op_getattr(
    core: Core, path: str, fh: Optional[int] = None
) -> Dict[str, Any]:
    # original_path = path
    # basename = os.path.basename(original_path)
    result = core.resolve_path(path)
    filenode_instance: Optional[Filenode] = None

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

    if filenode_instance:
        stat_db: Dict[str, Union[int, float]] = {
            "st_atime": filenode_instance.atime,
            "st_birthtime": filenode_instance.ctime,
            "st_ctime": filenode_instance.ctime,
            "st_mtime": filenode_instance.mtime,
            "st_size": filenode_instance.size,
        }

        attributes = {**attributes, **stat_db}

    return attributes
