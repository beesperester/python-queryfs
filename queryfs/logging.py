from typing import Any


def format_log_entry(*args: Any, **kwargs: Any) -> str:
    result = ""

    if args:
        result = "::".join(args)

    if kwargs:
        result += " -> " + " ".join(
            [f"{key}='{value}'" for key, value in kwargs.items()]
        )

    return result