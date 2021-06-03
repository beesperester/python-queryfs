from __future__ import annotations

from collections import OrderedDict
from typing import Optional
from queryfs.db.schema import Schema


class Directory(Schema):
    table_name: str = "directories"
    fields: OrderedDict[str, str] = OrderedDict(
        {
            "id": "integer primary key autoincrement",
            "name": "text",
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
    directory_id: Optional[int] = None
