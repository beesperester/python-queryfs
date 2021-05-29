from __future__ import annotations

import sqlite3
import logging

from contextlib import closing
from collections import OrderedDict
from typing import Dict, Iterable, List, Any, Optional, Type, TypeVar

T = TypeVar("T", bound="Schema")


class NotFoundException(Exception):
    ...


class Schema:
    # db specific attributes
    table_name: str
    fields: OrderedDict[str, str]

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

    def __init__(self) -> None:
        for key, value in self.fields.items():
            if value.lower().startswith("text"):
                setattr(self, key, "")
            elif value.lower().startswith("integer"):
                setattr(self, key, 0)

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


def create_table(db_name: str, model: Type[T]) -> None:
    with sqlite3.connect(db_name) as connection:
        with closing(connection.cursor()) as cursor:
            query = (model.create_schema_exists_query(), [model.table_name])

            logging.info(query)

            table = cursor.execute(
                *query,
            ).fetchone()

            if not table:
                query = model.create_schema_query()

                logging.info(query)

                cursor.execute(query)


def insert(db_name: str, schema: Type[T], **kwargs: Any) -> T:
    with sqlite3.connect(db_name) as connection:
        with closing(connection.cursor()) as cursor:
            data = OrderedDict(
                {
                    key: value
                    for key, value in kwargs.items()
                    if key in schema.fields.keys()
                }
            )

            query = (
                schema.create_insert_query(data.keys()),
                list(data.values()),
            )

            logging.info(query)

            cursor.execute(*query)

            item_id = cursor.lastrowid

    return fetch_one_by_id(db_name, schema, item_id)


def fetch_by(db_name: str, schema: Type[T], **kwargs: Any) -> sqlite3.Cursor:
    with sqlite3.connect(db_name) as connection:
        # with closing(connection.cursor()) as cursor:
        cursor = connection.cursor()

        data = OrderedDict(
            {
                key: value
                for key, value in kwargs.items()
                if key in schema.fields.keys()
            }
        )

        query = (
            schema.create_fetch_by_query(data.keys()),
            list(data.values()),
        )

        logging.info(query)

        return cursor.execute(*query)


def fetch_one_by(db_name: str, schema: Type[T], **kwargs: Any) -> T:
    cursor = fetch_by(db_name, schema, **kwargs)

    result = cursor.fetchone()

    logging.info(result)

    cursor.close()

    if result:
        model = schema()

        model.hydrate(*result)

        return model

    raise NotFoundException()


def fetch_one_by_id(db_name: str, schema: Type[T], id: int) -> T:
    return fetch_one_by(db_name, schema, id=id)


def fetch_many_by(db_name: str, schema: Type[T], **kwargs: Any) -> List[T]:
    cursor = fetch_by(db_name, schema, **kwargs)

    result = cursor.fetchall()

    logging.info(result)

    cursor.close()

    items: List[T] = []

    for data in result:
        item = schema()
        item.hydrate(*data)
        items.append(item)

    return items


def fetch_all(db_name: str, schema: Type[T]) -> List[T]:
    return fetch_many_by(db_name, schema)


def update(db_name: str, schema: Type[T], id: int, **kwargs: Any) -> T:
    with sqlite3.connect(db_name) as connection:
        with closing(connection.cursor()) as cursor:
            data = OrderedDict(
                {
                    key: value
                    for key, value in kwargs.items()
                    if key in schema.fields.keys()
                }
            )

            query = (
                schema.create_update_query(data.keys()),
                [*list(data.values()), id],
            )

            logging.info(query)

            cursor.execute(*query)

    return fetch_one_by_id(db_name, schema, id)


def delete(db_name: str, schema: Type[T], id: int) -> None:
    with sqlite3.connect(db_name) as connection:
        with closing(connection.cursor()) as cursor:
            query = (schema.create_delete_query(), [id])

            logging.info(query)

            cursor.execute(*query)
