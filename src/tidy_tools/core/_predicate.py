import functools
from typing import Callable
from typing import Sequence

from pyspark.sql import Column
from pyspark.sql import functions as F
from tidy_tools._types import ColumnReference


def _reference_column(func: Callable):
    @functools.wraps(func)
    def decorator(column: ColumnReference, *args, **kwargs) -> Column:
        if not isinstance(column, Column):
            column = F.col(column)
        return func(column, *args, **kwargs)

    return decorator


@_reference_column
def is_null(
    column: ColumnReference, _defaults: tuple[str] = (r"\s*", r"\bN/A\b")
) -> Column:
    """
    Predicate for identifying null values.

    Parameters
    ----------
    column : ColumnReference
        Reference to PySpark column.
    _defaults : tuple[str]
        Default values representing null. By default, checks for whitespace values and "N/A".

    Returns
    -------
    Column
        PySpark expression evaluating to boolean.
    """
    return column.isNull() | column.rlike(f"^({'|'.join(_defaults)})$")


@_reference_column
def is_regex_match(column: ColumnReference, pattern: str) -> Column:
    """
    Predicate for identifying a substring in a column.

    Parameters
    ----------
    column : ColumnReference
        Reference to PySpark column.
    pattern : str
        Regular expression. Must be compiled according to `re` library.

    Returns
    -------
    Column
        PySpark expression evaluating to boolean.
    """
    return column.rlike(pattern)


@_reference_column
def is_member(column: ColumnReference, elements: Sequence) -> Column:
    """
    Predicate for identifying values within a collection of elements.

    Parameters
    ----------
    column : ColumnReference
        Reference to PySpark column.
    elements : Sequence
        Collection of items expected to exist in any/all column(s).

    Returns
    -------
    Column
        PySpark expression evaluating to boolean.
    """
    return column.isin(elements)


@_reference_column
def is_between(column: ColumnReference, boundaries: Sequence) -> Column:
    """
    Predicate for identifying values within the specified boundaries.

    Parameters
    ----------
    column : ColumnReference
        Reference to PySpark column.
    boundaries : Sequence
        Sequence containing (<lower_bound>, <upper_bound>).

    Returns
    -------
    Column
        PySpark expression evaluating to boolean.
    """
    return column.between(*boundaries)
