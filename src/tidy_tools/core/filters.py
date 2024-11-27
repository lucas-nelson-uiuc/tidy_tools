import re
import warnings

from pyspark.sql import DataFrame

from tidy_tools.core._types import ColumnReference
from tidy_tools.core._constructor import construct_query
from tidy_tools.core import _predicate


def filter_nulls(
    self: DataFrame,
    *columns: ColumnReference,
    strict: bool = False,
    invert: bool = True,
) -> DataFrame:
    """Remove nulls from a DataFrame across any or all column(s)"""
    query = construct_query(
        *columns, predicate=_predicate.is_null, strict=strict, invert=invert
    )
    return self.filter(query)


def filter_regex(
    self: DataFrame,
    *columns: ColumnReference,
    regex: tuple[str],
    strict: bool = False,
    invert: bool = False,
) -> DataFrame:
    """Keep all observations across any or all columns that match the provided regular expression(s)."""
    assert all(isinstance(r, str) for r in regex)
    assert all(re.compile(r) for r in regex)
    query = construct_query(
        *columns,
        predicate=_predicate.is_regex_match,
        regex=regex,
        strict=strict,
        invert=invert,
    )
    return self.filter(query)


def filter_elements(
    self: DataFrame,
    *columns: ColumnReference,
    elements: list | tuple | dict,
    in_range: bool = False,
    strict: bool = False,
    invert: bool = False,
) -> DataFrame:
    """Keep all observations across any or all columns that exist within the provided bounds."""
    if in_range:
        assert (
            len(elements) == 2
        ), "If specifying a range, there must only be two elements: [<lower bound>, <upper bound>]"
        if isinstance(elements, dict):
            elements = elements.values()
        if elements != sorted(elements):
            warnings.warn("Elements were sorted by default.")

    query = construct_query(
        *columns,
        predicate=_predicate.is_member,
        elements=elements,
        membership_func="between" if in_range else "isin",
        strict=strict,
        invert=invert,
    )
    return self.filter(query)
