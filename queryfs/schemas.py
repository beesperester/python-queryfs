from __future__ import annotations


from collections import OrderedDict
from typing import List, Optional, TypeVar

from queryfs.db import Schema, Session, Relation

T = TypeVar("T", bound="Schema")


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

    def files(self, session: Session) -> List[File]:
        return Relation.one_to_many(self, session, File, "id", "directory_id")


class File(Schema):
    table_name: str = "files"
    fields: OrderedDict[str, str] = OrderedDict(
        {
            "id": "integer primary key autoincrement",
            "name": "text",
            "directory_id": "integer null",
            "filenode_id": "integer",
        }
    )

    id: int = 0
    name: str = ""
    directory_id: Optional[int] = 0
    filenode_id: int = 0

    def filenode(self, session: Session) -> Optional[Filenode]:
        return Relation.one_to_one(
            self, session, Filenode, "filenode_id", "id"
        )


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
