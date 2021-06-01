from collections import OrderedDict
from typing import Optional
from queryfs.db.schema import Schema


class File(Schema):
    table_name: str = "files"
    fields: OrderedDict[str, str] = OrderedDict(
        {
            "id": "integer primary key autoincrement",
            "name": "text",
            "hash": "text",
            "ctime": "real",
            "atime": "real",
            "mtime": "real",
            "size": "integer",
            "directory_id": "integer null",
        }
    )

    id: int = 0
    name: str = ""
    hash: str = ""
    ctime: float = 0.0
    atime: float = 0.0
    mtime: float = 0.0
    size: int = 0
    directory_id: Optional[int] = 0
