from __future__ import annotations

import sqlite3
import logging
import os

from functools import reduce
from contextlib import closing
from collections import OrderedDict
from typing import (
    Generic,
    List,
    Any,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Dict,
    Union,
)
from queryfs import PathLike

T = TypeVar("T", bound="Schema")
S = TypeVar("S", bound="Schema")
logger = logging.getLogger("db")


class NotFoundException(Exception):
    ...


class QueryBuilderError(Exception):
    ...


# class Statement:
#     TYPE_KEYWORD: int = 1
#     TYPE_FILTER: int = 10

#     def __init__(
#         self,
#         statement_type: int,
#         statement: str,
#         values: Optional[List[Any]] = None,
#     ) -> None:
#         if values is None:
#             values = []

#         self.statement_type = statement_type
#         self.statement = statement
#         self.values = values


class Constraint:
    def __init__(self, field: str, type: str, value: Any) -> None:
        self.field = field
        self.type = type
        self.value = value

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} field='{self.field}' type='{self.type}' value='{self.value}' at {hex(id(self))}>"


class Relation:
    TYPE_ONE_TO_MANY: str = "one to many"

    def __init__(
        self, schema: Type[T], own_key: str, other_key: str, type: str
    ) -> None:
        self.schema = schema
        self.own_key = own_key
        self.other_key = other_key
        self.type = type


class Schema:
    # db specific attributes
    table_name: str = ""
    fields: OrderedDict[str, str] = OrderedDict()
    relations: Dict[str, Relation] = {}

    # object methods

    def __init__(self, *args: Any) -> None:
        for key, value in self.fields.items():
            if value.lower().startswith("text"):
                setattr(self, key, "")
            elif value.lower().startswith("integer"):
                setattr(self, key, 0)
            elif value.lower().startswith("real"):
                setattr(self, key, 0.0)

            if "null" in value.lower():
                setattr(self, key, None)

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
            if index < len(self.fields.keys()) and arg is not None:
                setattr(self, list(self.fields.keys())[index], arg)

    def get_constraint(self, name: str) -> Constraint:
        relation = self.relations.get(name)

        if relation:
            return Constraint(
                relation.other_key, "is", getattr(self, relation.own_key)
            )

        raise QueryBuilderError(
            f"Unable to find relation '{name}' in schema '{self.__class__.__name__}'"
        )


class QueryBuilder(Generic[T]):
    def __init__(
        self,
        session: Session,
        schema: Type[T],
        constraints: Optional[List[Constraint]] = None,
    ) -> None:
        if constraints is None:
            constraints = []

        self.session = session
        self.schema = schema
        self.constraints = constraints

        self.statement: Optional[
            Tuple[str, List[Union[str, int, float]]]
        ] = None

        # self.query: List[Statement] = []
        self.cursor: Optional[sqlite3.Cursor] = None

    def get_last_row_id(self) -> Optional[int]:
        if self.cursor:
            last_row_id = self.cursor.lastrowid

            self.close()

            return last_row_id

    def constraint_by(self, constraint: Constraint) -> QueryBuilder[T]:
        self.constraints.append(constraint)

        return self

    def select(self, *args: str) -> QueryBuilder[T]:
        fields = list(self.schema.fields.keys())

        if args:
            fields = [field for field in args if field in fields]

        self.statement = (
            " ".join(
                [
                    "SELECT",
                    ", ".join(fields),
                    "FROM",
                    self.schema.table_name,
                ]
            ),
            [],
        )

        return self

    def delete(self) -> QueryBuilder[T]:
        self.statement = (
            " ".join(
                [
                    f"DELETE FROM {self.schema.table_name}",
                ]
            ),
            [],
        )

        return self

    def insert(self, **kwargs: Any) -> QueryBuilder[T]:
        fields: str = ", ".join([x for x in kwargs.keys()])
        values: str = ", ".join(["?" for _ in kwargs.keys()])

        self.statement = (
            " ".join(
                [
                    f"INSERT INTO {self.schema.table_name}",
                    f"({fields}) VALUES ({values})",
                ]
            ),
            list(kwargs.values()),
        )

        return self

    def update(self, **kwargs: Any) -> QueryBuilder[T]:
        fields: str = ", ".join([x for x in kwargs.keys()])
        values: str = ", ".join(["?" for _ in kwargs.keys()])

        self.statement = (
            " ".join(
                [
                    f"UPDATE {self.schema.table_name}",
                    f"SET ({fields}) = ({values})",
                ]
            ),
            list(kwargs.values()),
        )

        return self

    def where(self, *args: Constraint) -> QueryBuilder[T]:
        # fields: str = ", ".join([x for x in kwargs.keys()])
        # values: str = ", ".join(["?" for _ in kwargs.keys()])

        self.constraints += args

        return self

    def build(self) -> Tuple[str, List[Union[str, int, float]]]:
        if self.statement:
            query_string, query_values = self.statement

            if self.constraints:
                constraints_grouped: Dict[str, List[Constraint]] = {}

                for constraint in self.constraints:
                    group = constraints_grouped.setdefault(constraint.type, [])

                    group.append(constraint)

                constraint_strings: List[str] = []

                values: List[Any] = []

                for (
                    constraint_type,
                    constraints,
                ) in constraints_grouped.items():
                    fields_string: str = ", ".join(
                        [x.field for x in constraints]
                    )
                    values_string: str = ", ".join(["?" for _ in constraints])

                    constraint_strings.append(
                        f"({fields_string}) {constraint_type} ({values_string})"
                    )
                    values += [x.value for x in constraints]

                constraint_string = " AND ".join(constraint_strings)

                query_string += " WHERE " + constraint_string
                query_values += values

            return (query_string, query_values)

        raise QueryBuilderError("Missing statement")

    def execute(self) -> QueryBuilder[T]:
        with self.session.connect() as connection:
            cursor = connection.cursor()
            query = self.build()

            logger.info(query)

            # empty query
            self.query = []

            self.cursor = cursor.execute(*query)

        return self

    def close(self) -> QueryBuilder[T]:
        if self.cursor:
            self.cursor.close()

            self.cursor = None

        return self

    def fetch_one(self) -> Optional[T]:
        if self.cursor:
            result = self.cursor.fetchone()

            self.close()

            if result:
                return self.schema(*result)

    def fetch_all(self) -> List[T]:
        if self.cursor:
            result = self.cursor.fetchall()

            self.close()

            return [self.schema(*x) for x in result]

        return []


class Session:
    def __init__(self, db_name: PathLike) -> None:
        self.db_name = db_name

    def query(self, schema: Type[T]) -> QueryBuilder[T]:
        return QueryBuilder(self, schema)

    def connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_name)

    def table_exists(self, schema: Type[T]) -> bool:
        with self.connect() as connection:
            with closing(connection.cursor()) as cursor:
                check_table_query = (
                    "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
                    [schema.table_name],
                )

                result = cursor.execute(*check_table_query).fetchone()

                if result:
                    return True

        return False

    def create_table(self, schema: Type[T]) -> None:
        if self.table_exists(schema):
            return

        with self.connect() as connection:
            with closing(connection.cursor()) as cursor:
                fields: List[str] = [
                    f"{key} {value.upper()}"
                    for key, value in schema.fields.items()
                ]

                # if schema.relations:
                #     for _, relation in schema.relations.items():
                #         fields.append(
                #             f"FOREIGN KEY ({relation.own_key}) REFERENCES {relation.schema.table_name}"
                #         )

                fields_string = ", ".join(fields)

                create_table_query: Tuple[str, List[Any]] = (
                    f"CREATE TABLE {schema.table_name} ({fields_string})",
                    [],
                )

                logger.info(create_table_query)

                cursor.execute(*create_table_query)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    class Test(Schema):
        table_name = "tests"
        fields = OrderedDict(
            {"id": "integer primary key autoincrement", "name": "text"}
        )

    if os.path.isfile("test.db"):
        os.unlink("test.db")

    session = Session("test.db")

    session.create_table(Test)

    # test insertion
    last_row_id = (
        session.query(Test).insert(name="foo").execute().get_last_row_id()
    )

    print(last_row_id)

    # test fetch one
    test = (
        session.query(Test)
        .select("id", "name")
        .where(Constraint("name", "=", "foo"))
        .execute()
        .fetch_one()
    )

    if test:
        print(test)

    # test fetch all
    test = session.query(Test).select().execute().fetch_all()

    print(test)

    # test update
    test = (
        session.query(Test)
        .update(name="bar")
        .where(Constraint("id", "is", 1))
        .execute()
        .close()
    )

    session.query(Test).delete().where(
        Constraint("id", "is", 1)
    ).execute().close()

    # query.create_table()

    # test = query.select("id").where(name="foobar").fetch_one()

    # print(test)
