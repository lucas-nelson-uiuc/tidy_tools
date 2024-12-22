from typing import Any
from typing import Callable
from typing import Sequence

from pyspark.sql import Column
from tidy_tools.core import _predicate


def validate_nulls(**kwargs: dict) -> Callable:
    """
    Return expression checking for null values in column.

    Parameters
    ----------
    **kwargs : dict
        Arbitrary number of keyword arguments. See `is_null` for more details.

    Returns
    -------
    Callable
        Constructs closure that can be called on column(s).
    """

    def closure(column: str) -> Column:
        return _predicate.is_null(column, **kwargs)

    return lambda column: closure(column)


def validate_pattern(pattern: str) -> Callable:
    """
    Return expression checking for pattern in column.

    Parameters
    ----------
    pattern : str
        Regular expression to check for in column.

    Returns
    -------
    Callable
        Constructs closure that can be called on column(s).
    """

    def closure(column: str) -> Column:
        return _predicate.is_regex_match(column, pattern=pattern)

    return closure


def validate_membership(elements: Sequence) -> Callable:
    """
    Return expression checking for membership in column.

    Parameters
    ----------
    elements : Sequence
        Collection containing value(s) to check for in column.

    Returns
    -------
    Callable
        Constructs closure that can be called on column(s).
    """

    def closure(column: str) -> Column:
        return _predicate.is_member(column, elements=elements)

    return closure


def validate_range(lower_bound: Any, upper_bound: Any) -> Callable:
    """
    Return expression checking for inclusion in column.

    Parameters
    ----------
    lower_bound : Any
        Least value to check for in column.
    upper_bound : Any
        Greatest value to check for in column.

    Returns
    -------
    Callable
        Constructs closure that can be called on column(s).
    """

    def closure(column: str) -> Column:
        return _predicate.is_between(column, boundaries=(lower_bound, upper_bound))

    return closure
