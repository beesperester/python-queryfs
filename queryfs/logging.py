import logging
import inspect

from typing import Any, Callable


def format_log_entry(*args: Any, **kwargs: Any) -> str:
    result = ""

    if args:
        result = " ".join([str(x) for x in args])

    if kwargs:
        result += " -> " + " ".join(
            [f"{key}='{value}'" for key, value in kwargs.items()]
        )

    return result


def logging_decorator(
    logger: logging.Logger, *decorator_args: str
) -> Callable[..., Any]:
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        signature = inspect.signature(func)

        _, *argument_names = list(signature.parameters.keys())

        def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            log_arguments = dict(zip(argument_names, args))

            logger.info(
                format_log_entry(
                    *decorator_args,
                    **log_arguments,
                    **kwargs,
                )
            )

            result = func(self, *args, **kwargs)

            logger.info(format_log_entry(*decorator_args, result=result))

            return result

        return wrapper

    return decorator
