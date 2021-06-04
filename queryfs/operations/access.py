import os
import errno

from queryfs.core import Core
from queryfs.schemas import File, Directory
from fuse import FuseOSError


def op_access(core: Core, path: str, amode: int) -> None:
    result = core.resolve_path(path)

    if isinstance(result, File):
        filenode_instance = result.filenode(core.session)

        if filenode_instance:
            resolved_path = core.blobs.joinpath(filenode_instance.hash)
        else:
            raise Exception("Missing Filenode")
    elif isinstance(result, Directory):
        return
    else:
        resolved_path = result

    if not os.access(resolved_path, amode):
        raise FuseOSError(errno.EACCES)
