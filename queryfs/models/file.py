from collections import OrderedDict
from typing import Optional

from queryfs.db.session import Session, Relation, Schema
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
    relations = {
        "filenode": Relation(
            Filenode, "filenode_id", "id", Relation.TYPE_ONE_TO_MANY
        )
    }

    id: int = 0
    name: str = ""
    directory_id: Optional[int] = 0
    filenode_id: int = 0

    def filenode(self, session: Session) -> Optional[Filenode]:
        return (
            session.query(Filenode)
            .select()
            .where(self.get_constraint("filenode"))
            .execute()
            .fetch_one()
        )
