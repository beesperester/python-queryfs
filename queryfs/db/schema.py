from __future__ import annotations

import logging

from collections import OrderedDict
from typing import Any, TypeVar

T = TypeVar("T", bound="Schema")

logger = logging.getLogger("db")


class Schema:
    # db specific attributes
    table_name: str = ""
    fields: OrderedDict[str, str] = OrderedDict()

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