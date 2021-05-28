import sqlite3

from typing import List, Optional, Tuple


class NotFoundException(Exception):
    ...


class File:
    def __init__(self, id: int, name: str, hash: str, lock: int) -> None:
        self.id = id
        self.name = name
        self.hash = hash
        self.lock = lock


def init(db_name: str) -> None:
    with sqlite3.connect("files.db") as connection:
        cursor = connection.cursor()
        files_table = cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name= ?",
            ("files",),
        ).fetchone()

        if not files_table:
            cursor.execute(
                "CREATE TABLE files (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, hash TEXT, lock INTEGER)"
            )


def readdir(db_name: str) -> List[File]:
    rows: List[File] = []

    with sqlite3.connect(db_name) as connection:
        cursor = connection.cursor()
        rows = [
            File(*x) for x in cursor.execute("SELECT * FROM files").fetchall()
        ]

    return rows


def lock(db_name: str, id: int, fh: int) -> None:
    with sqlite3.connect(db_name) as connection:
        cursor = connection.cursor()
        cursor.execute(
            "UPDATE files SET lock = ? WHERE id = ?",
            (
                fh,
                id,
            ),
        )


def unlock(db_name: str, id: int) -> None:
    with sqlite3.connect(db_name) as connection:
        cursor = connection.cursor()
        cursor.execute("UPDATE files SET lock = 0 WHERE id = ?", (id,))


def create(db_name: str, name: str, fh: int) -> File:
    with sqlite3.connect(db_name) as connection:
        cursor = connection.cursor()
        cursor.execute(
            "INSERT INTO files (name, hash, lock) VALUES (?, ?, ?)",
            (
                name,
                "",
                fh,
            ),
        )

        return File(cursor.lastrowid, name, "", 1)


def get_file_by_name(db_name: str, name: str) -> Optional[File]:
    with sqlite3.connect(db_name) as connection:
        cursor = connection.cursor()

        result = cursor.execute(
            "SELECT * FROM files WHERE name = ?", (name,)
        ).fetchone()

        if result:
            return File(*result)


def get_file_by_hash(db_name: str, hash: str) -> Optional[File]:
    with sqlite3.connect(db_name) as connection:
        cursor = connection.cursor()

        result = cursor.execute(
            "SELECT * FROM files WHERE hash = ?", (hash,)
        ).fetchone()

        if result:
            return File(*result)


def get_file_by_lock(db_name: str, lock: int) -> Optional[File]:
    with sqlite3.connect(db_name) as connection:
        cursor = connection.cursor()

        result = cursor.execute(
            "SELECT * FROM files WHERE lock = ?", (lock,)
        ).fetchone()

        if result:
            return File(*result)


def release(db_name: str, name: str, hash: str) -> None:
    with sqlite3.connect(db_name) as connection:
        cursor = connection.cursor()

        previous = get_file_by_name(db_name, name)

        if previous:
            cursor.execute(
                "UPDATE files SET hash = ?, lock = 0 WHERE id = ?",
                (
                    hash,
                    previous.id,
                ),
            )
        else:
            cursor.execute(
                f"INSERT INTO files (name, hash, lock) VALUES (?, ?, ?)",
                (
                    name,
                    hash,
                    0,
                ),
            )
