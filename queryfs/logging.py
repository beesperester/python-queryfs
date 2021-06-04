import logging
from typing import Any, List


def format_entry(*args: Any, **kwargs: Any) -> str:
    strings: List[str] = []

    if args:
        strings.append(":".join(args))

    if kwargs:
        strings.append(
            " ".join([f"{key}='{value}'" for key, value in kwargs.items()])
        )

    return " -> ".join(strings)
