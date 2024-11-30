import functools
from typing import Callable
from typing import Iterable

from pyspark.sql import Column
from pyspark.sql import functions as F
from tidy_tools.core._types import ColumnReference


def _reference_column(func: Callable):
    @functools.wraps(func)
    def decorator(column: ColumnReference, *args, **kwargs) -> Column:
        if not isinstance(column, Column):
            column = F.col(column)
        return func(column, *args, **kwargs)

    return decorator


@_reference_column
def is_null(column: ColumnReference, values: tuple[str] = (r"\s*",)) -> Column:
    """Predicate for identifying null values."""
    return column.isNull() | column.rlike(f"^({'|'.join(values)})$")


@_reference_column
def is_regex_match(column: ColumnReference, values: tuple[str]) -> Column:
    """Predicate for identifying a substring in a column."""
    return column.rlike(rf"({'|'.join(values)})")


@_reference_column
def is_member(
    column: ColumnReference, elements: Iterable, membership_func: str
) -> Column:
    """Predicate for identifying values within the specified boundaries."""
    return getattr(column, membership_func)(*elements)
