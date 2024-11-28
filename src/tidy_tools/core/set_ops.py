from __future__ import annotations
from typing import Callable, Optional, Iterable
import functools

from pyspark.sql import DataFrame

from tidy_tools.core._types import ColumnReference


def safely_join(*checks: Callable):
    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(self, other, on, how, *args, **kwargs):
            self_columns = self.columns
            other_columns = other.columns

            on_columns: list[str] = None
            if isinstance(on, str):
                on_columns = [on]
            if isinstance(on, list):
                on_columns = [column for column in on if isinstance(column, str)]

            # TODO: something about Column.eqNullSafe

        return wrapper

    return decorator


def _join(
    relationship: Optional[str] = "one-to-one",
    unmatched: Optional[str] = "ignore",
    suffix: Optional[tuple[str]] = ("left", "right"),
) -> Callable:
    JOIN_RELATIONSHIP: tuple[str] = (
        "one-to-one",
        "many-to-one",
        "one-to-many",
        "many-to-many",
    )

    UNMATCHED_VALUES: tuple[str] = (
        "ignore",
        "drop",
        "error"
    )

    relationship = relationship.strip().lower().replace(" ", "-")
    assert (
        relationship in JOIN_RELATIONSHIP
    ), f"Unexpected value for `relationship`. Please pass one of: {', '.join(JOIN_RELATIONSHIP)}"
    assert (
        unmatched in UNMATCHED_VALUES
    ), f"Unexpected value for `unmatched`. Please pass one of: {', '.join(UNMATCHED_VALUES)}"
    assert all(
        (len(suffix) == 2, isinstance(suffix, tuple), all(isinstance(s, str) for s in suffix))
    ), "Unexpected value for `suffix`. Please pass a tuple of two characters (e.g. ('left', 'right') or ('x', 'y'))"

    @safely_join(relationship=relationship, unmatched=unmatched, suffix=suffix)
    def closure(
        self: DataFrame,
        other: DataFrame,
        on: ColumnReference | Iterable[ColumnReference],
        how: str,
    ) -> DataFrame:
        return self.join(other, on=on, how=how)

    return closure


def join(
    self: DataFrame,
    other: DataFrame,
    on: ColumnReference,
    how: str,
    relationship: Optional[str] = "one-to-one",
    unmatched: Optional[str] = "ignore",
    suffix: Optional[tuple[str]] = ("left", "right"),
) -> DataFrame:
    join_func = _join(relationship=relationship, unmatched=unmatched, suffix=suffix)
    return join_func(self, other, on, how)


def inner_join(
    self: DataFrame,
    other: DataFrame,
    on: ColumnReference,
    relationship: Optional[str] = "one-to-one",
    suffix: str = ("left", "right"),
    unmatched: str = "error",
) -> DataFrame:
    """Safely perform an inner join between DataFrame objects."""
    join_func = _join(relationship=relationship, suffix=suffix, unmatched=unmatched)
    return join_func(self, other, on=on, how="inner")


def left_join(
    self: DataFrame,
    other: DataFrame,
    on: ColumnReference,
    relationship: Optional[str] = "one-to-one",
) -> DataFrame:
    """Safely perform an inner join between DataFrame objects."""
    join_func = _join(relationship=relationship)
    return join_func(self, other, on=on, how="left")


def right_join(
    self: DataFrame,
    other: DataFrame,
    on: ColumnReference,
    relationship: Optional[str] = "one-to-one",
) -> DataFrame:
    """Safely perform an inner join between DataFrame objects."""
    join_func = _join(relationship=relationship)
    return join_func(self, other, on=on, how="right")


def outer_join(
    self: DataFrame,
    other: DataFrame,
    on: ColumnReference,
    relationship: Optional[str] = "one-to-one",
) -> DataFrame:
    """Safely perform an inner join between DataFrame objects."""
    join_func = _join(relationship=relationship)
    return join_func(self, other, on=on, how="outer")


def full_join(
    self: DataFrame,
    other: DataFrame,
    on: ColumnReference,
    relationship: Optional[str] = "one-to-one",
) -> DataFrame:
    """Safely perform an inner join between DataFrame objects."""
    return outer_join(self, other, on, relationship)


def concat(
    *dataframes: DataFrame,
    merge_func: Callable = DataFrame.unionByName,
    **merge_options: dict
) -> DataFrame:
    merge_func = functools.partial(merge_func, **merge_options)
    return functools.reduce(merge_func, dataframes)
