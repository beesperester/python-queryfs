from __future__ import annotations
from os import PathLike

import sqlite3
import logging

from contextlib import closing
from collections import OrderedDict
from pathlib import Path
from typing import Dict, Iterable, List, Any, Optional, Type, TypeVar, Union
from queryfs import PathLike

T = TypeVar("T", bound="Schema")

logger = logging.getLogger("db")


class NotFoundException(Exception):
    ...


class Schema:
    # db specific attributes
    table_name: str = ""
    fields: OrderedDict[str, str] = OrderedDict()

    @classmethod
    def get_field_names(cls) -> List[str]:
        return list(cls.fields.keys())

    @classmethod
    def get_field_names_select_string(cls) -> str:
        return ", ".join(cls.get_field_names())

    @classmethod
    def create_schema_query(cls) -> str:
        fields = ", ".join(
            [f"{key} {value.upper()}" for key, value in cls.fields.items()]
        )

        return f"CREATE TABLE {cls.table_name} ({fields})"

    @classmethod
    def create_schema_exists_query(cls) -> str:
        return "SELECT name FROM sqlite_master WHERE type='table' AND name = ?"

    @classmethod
    def create_insert_query(cls, field_names: Iterable[str]) -> str:
        field_names_string = ", ".join(field_names)
        values_string = ", ".join(["?" for _ in field_names])

        return f"INSERT INTO {cls.table_name} ({field_names_string}) VALUES ({values_string})"

    @classmethod
    def create_fetch_by_query(cls, field_names: Iterable[str]) -> str:
        query_string = f"SELECT {cls.get_field_names_select_string()} FROM {cls.table_name}"

        field_names_string = ", ".join(field_names)
        values_string = ", ".join(["?" for _ in field_names])

        if field_names:
            query_string += (
                f" WHERE ({field_names_string}) = ({values_string})"
            )

        return query_string

    @classmethod
    def create_update_query(cls, field_names: Iterable[str]) -> str:
        field_names_strings = ", ".join(field_names)
        values_string = ", ".join(["?" for _ in field_names])

        return f"UPDATE {cls.table_name} SET ({field_names_strings}) = ({values_string}) WHERE id = ?"

    @classmethod
    def create_delete_query(cls) -> str:
        return f"DELETE FROM {cls.table_name} WHERE id = ?"

    # object methods

    def __init__(self, *args: Any) -> None:
        for key, value in self.fields.items():
            if value.lower().startswith("text"):
                setattr(self, key, "")
            elif value.lower().startswith("integer"):
                setattr(self, key, 0)

        self.hydrate(*args)

    def __repr__(self) -> str:
        fields = list(self.fields.keys())

        info = " ".join(
            [f"{x}='{getattr(self, x)}'" for x in fields if hasattr(self, x)]
        )

        return f"<{self.__class__.__name__} {info} at {hex(id(self))}>"

    # data hydration

    def hydrate(self, *args: Any) -> None:
        for index, arg in enumerate(args):
            if index < len(self.get_field_names()) and arg is not None:
                setattr(self, self.get_field_names()[index], arg)