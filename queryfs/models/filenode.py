from collections import OrderedDict
from queryfs.db.session import Schema


class Filenode(Schema):
    table_name: str = "filenodes"
    fields: OrderedDict[str, str] = OrderedDict(
        {
            "id": "integer primary key autoincrement",
            "hash": "text",
            "ctime": "real",
            "atime": "real",
            "mtime": "real",
            "size": "integer",
            "previous_filenode_id": "integer",
        }
    )

    id: int = 0
    hash: str = ""
    ctime: float = 0.0
    atime: float = 0.0
    mtime: float = 0.0
    size: int = 0
    previous_filenode_id: int = 0
