from collections import OrderedDict
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
        }
    )

    id: int = 0
    name: str = ""
    hash: str = ""
    ctime: float = 0.0
    atime: float = 0.0
    mtime: float = 0.0
    size: int = 0
