import re
from typing import Any
from typing import Sequence

from pyspark.sql import DataFrame
from tidy_tools._types import ColumnReference
from tidy_tools.core import _predicate
from tidy_tools.core._constructor import construct_query


def filter_nulls(
    self: DataFrame,
    *columns: ColumnReference,
    strict: bool = False,
    invert: bool = False,
) -> DataFrame:  # numpydoc ignore=PR09
    """
    Keep all observations that represent null across all column(s).

    Parameters
    ----------
    self : DataFrame
        Object inheriting from PySpark DataFrame.
    *columns : ColumnReference
        Arbitrary number of column references. All columns must exist in `self`. If none
        are passed, all columns are used in filter.
    strict : bool
        Should condition be true for all column(s)?
    invert : bool
        Should observations that meet condition be kept (False) or removed (True)?

    Returns
    -------
    DataFrame
        Observations that represent null across all column(s).
    """
    query = construct_query(
        *columns or self.columns,
        predicate=_predicate.is_null,
        strict=strict,
        invert=invert,
    )
    return self.filter(query)


def filter_regex(
    self: DataFrame,
    *columns: ColumnReference,
    pattern: str,
    strict: bool = False,
    invert: bool = False,
) -> DataFrame:  # numpydoc ignore=PR09
    """
    Keep all observations that match the regular expression across all column(s).

    Parameters
    ----------
    self : DataFrame
        Object inheriting from PySpark DataFrame.
    *columns : ColumnReference
        Arbitrary number of column references. All columns must exist in `self`. If none
        are passed, all columns are used in filter.
    pattern : str
        Regular expression. Must be compiled according to `re` library.
    strict : bool
        Should condition be true for all column(s)?
    invert : bool
        Should observations that meet condition be kept (False) or removed (True)?

    Returns
    -------
    DataFrame
        Observations that match the regular expression across all column(s).
    """
    try:
        re.compile(pattern)
    except Exception as e:
        print(f"Cannot compile {pattern=} as regular expression. Raises: '{e}'")
    query = construct_query(
        *columns or self.columns,
        predicate=_predicate.is_regex_match,
        pattern=pattern,
        strict=strict,
        invert=invert,
    )
    return self.filter(query)


def filter_elements(
    self: DataFrame,
    *columns: ColumnReference,
    elements: Sequence,
    strict: bool = False,
    invert: bool = False,
) -> DataFrame:  # numpydoc ignore=PR09
    """
    Keep all observations that exist within elements across all column(s).

    Parameters
    ----------
    self : DataFrame
        Object inheriting from PySpark DataFrame.
    *columns : ColumnReference
        Arbitrary number of column references. All columns must exist in `self`. If none
        are passed, all columns are used in filter.
    elements : Sequence
        Collection of items expected to exist in any/all column(s).
    strict : bool
        Should condition be true for all column(s)?
    invert : bool
        Should observations that meet condition be kept (False) or removed (True)?

    Returns
    -------
    DataFrame
        Observations that exist within range across all column(s).
    """
    query = construct_query(
        *columns or self.columns,
        predicate=_predicate.is_member,
        elements=elements,
        strict=strict,
        invert=invert,
    )
    return self.filter(query)


def filter_range(
    self: DataFrame,
    *columns: ColumnReference,
    boundaries: Sequence[Any],
    strict: bool = False,
    invert: bool = False,
) -> DataFrame:  # numpydoc ignore=PR09
    """
    Keep all observations that exist within range across all column(s).

    Parameters
    ----------
    self : DataFrame
        Object inheriting from PySpark DataFrame.
    *columns : ColumnReference
        Arbitrary number of column references. All columns must exist in `self`. If none
        are passed, all columns are used in filter.
    boundaries : Sequence[Any]
        Bounds of range. Must be of same type and in ascending order.
    strict : bool
        Should condition be true for all column(s)?
    invert : bool
        Should observations that meet condition be kept (False) or removed (True)?

    Returns
    -------
    DataFrame
        Observations that exist within range across all column(s).
    """
    try:
        lower_bound, upper_bound = boundaries
        assert type(lower_bound) is type(upper_bound)
        assert lower_bound < upper_bound
    except AssertionError:
        raise AssertionError(
            f"Boundaries must be same type and in ascending order. Received ({lower_bound=} ({type(lower_bound)}), {upper_bound=} ({type(upper_bound)}))"
        )
    query = construct_query(
        *columns or self.columns,
        predicate=_predicate.is_between,
        boundaries=boundaries,
        strict=strict,
        invert=invert,
    )
    return self.filter(query)
