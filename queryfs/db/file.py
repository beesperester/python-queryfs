from os import PathLike
from typing import Dict, List, Any, Optional, Union
from pathlib import Path
from collections import OrderedDict
from queryfs import db


class File(db.Schema):
    table_name: str = "files"
    fields: OrderedDict[str, str] = OrderedDict(
        {
            "id": "integer primary key autoincrement",
            "name": "text",
            "hash": "text",
            "fh": "integer",
        }
    )

    id: int
    name: str
    hash: str
    text: str
    fh: int


def fetch_one_by_name(db_name: Union[str, Path], name: str) -> File:
    return db.fetch_one_by(db_name, File, name=name)


def update_fh(db_name: Union[str, Path], file: File, fh: int) -> File:
    return db.update(db_name, File, file.id, fh=fh)


def update_hash(db_name: Union[str, Path], file: File, hash: str) -> File:
    return db.update(db_name, File, file.id, hash=hash)


def fetch_one_by_fh(db_name: Union[str, Path], fh: int) -> File:
    return db.fetch_one_by(db_name, File, fh=fh)


if __name__ == "__main__":
    import os
    import logging

    logging.basicConfig(level=logging.DEBUG)

    if os.path.isfile("test.db"):
        os.unlink("test.db")

    db.create_table("test.db", File)

    file = db.insert("test.db", File, name="foobar.txt")

    file = update_fh("test.db", file, 5)

    print(fetch_one_by_fh("test.db", 5))

    # print(file)

    # files = db.fetch_all("test.db", File)

    # print(files)

    # db.update("test.db", File, 1, hash="asdf")

    # db.delete("test.db", File, 1)

    # db.fetch_all("test.db", File)
