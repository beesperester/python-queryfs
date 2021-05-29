from os import PathLike
from typing import List, Any, Tuple
from pathlib import Path
from collections import OrderedDict
from queryfs import db, PathLike


class File(db.Schema):
    table_name: str = "files"
    fields: OrderedDict[str, str] = OrderedDict(
        {
            "id": "integer primary key autoincrement",
            "name": "text",
            "hash": "text",
            "fh": "integer",
            "ctime": "real",
            "atime": "real",
            "mtime": "real",
        }
    )

    id: int
    name: str
    hash: str
    text: str
    fh: int
    ctime: float
    atime: float
    mtime: float


def insert(db_name: PathLike, **kwargs: Any) -> File:
    return db.insert(db_name, File, **kwargs)


def fetch_one_by_name(db_name: PathLike, name: str) -> File:
    return db.fetch_one_by(db_name, File, name=name)


def fetch_many_by_hash(db_name: PathLike, hash: str) -> List[File]:
    return db.fetch_many_by(db_name, File, hash=hash)


def fetch_one_by_fh(db_name: PathLike, fh: int) -> File:
    return db.fetch_one_by(db_name, File, fh=fh)


def update(db_name: PathLike, file: File, **kwargs: Any) -> File:
    return db.update(db_name, File, file.id, **kwargs)


# def update_fh(db_name: PathLike, file: File, fh: int) -> File:
#     return db.update(db_name, File, file.id, fh=fh)


# def update_hash(db_name: PathLike, file: File, hash: str) -> File:
#     return db.update(db_name, File, file.id, hash=hash)


# def update_utimens(
#     db_name: PathLike, file: File, times: Tuple[int, int]
# ) -> File:
#     return db.update(db_name, File, file.id, atime=times[0], mtime=times[1])


def delete(db_name: PathLike, file: File) -> None:
    return db.delete(db_name, File, file.id)
