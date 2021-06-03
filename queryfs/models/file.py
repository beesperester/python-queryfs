from collections import OrderedDict
from typing import Optional

from queryfs.db.schema import Schema
from queryfs.db.session import Session
from queryfs.models.filenode import Filenode
from queryfs.db.session import Constraint


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


def fetch_filenode(
    session: Session, file_instance: File
) -> Optional[Filenode]:
    return (
        session.query(Filenode)
        .select()
        .where(Constraint("id", "is", file_instance.filenode_id))
        .execute()
        .fetch_one()
    )
