from typing import Callable
from enum import Enum
import re

from attrs import define

from pyspark.sql import types as T


class PySparkTypes(Enum):
    STRING = (T.StringType, T.VarcharType, T.CharType)
    NUMERIC = (
        T.ByteType,
        T.ShortType,
        T.IntegerType,
        T.LongType,
        T.FloatType,
        T.DoubleType,
        T.DecimalType,
    )
    TEMPORAL = (T.DateType, T.TimestampType, T.TimestampNTZType)
    INTERVAL = (T.YearMonthIntervalType, T.DayTimeIntervalType)
    COMPLEX = (T.ArrayType, T.MapType, T.StructType)


@define
class ColumnSelector:
    """Define generic class for selecting columns based on expressions."""

    expression: Callable

    def __or__(self, other) -> "ColumnSelector":
        if not isinstance(other, ColumnSelector):
            return NotImplemented
        return ColumnSelector(lambda col: self.expression(col) or other.expression(col))

    def __xor__(self, other) -> "ColumnSelector":
        if not isinstance(other, ColumnSelector):
            return NotImplemented
        return ColumnSelector(lambda col: self.expression(col) ^ other.expression(col))

    def __and__(self, other) -> "ColumnSelector":
        if not isinstance(other, ColumnSelector):
            return NotImplemented
        return ColumnSelector(
            lambda col: self.expression(col) and other.expression(col)
        )

    def __sub__(self, other) -> "ColumnSelector":
        if not isinstance(other, ColumnSelector):
            return NotImplemented
        return ColumnSelector(
            lambda col: self.expression(col) and not other.expression(col)
        )

    def __ror__(self, other) -> "ColumnSelector":
        return self.__or__(other)

    def __rand__(self, other) -> "ColumnSelector":
        return self.__and__(other)

    def __invert__(self) -> "ColumnSelector":
        return not self.expression

    def __call__(self, column: str) -> bool:
        return self.expression(column)


def _name_selector(pattern: str, match_func: Callable):
    def closure(sf: T.StructField) -> bool:
        return match_func(sf.name, pattern)

    return ColumnSelector(expression=closure)


def _dtype_selector(dtype: T.DataType | tuple[T.DataType]):
    def closure(sf: T.StructField) -> bool:
        return isinstance(sf.dataType, dtype)

    return ColumnSelector(expression=closure)


def string() -> ColumnSelector:
    return _dtype_selector(PySparkTypes.STRING.value)


def numeric() -> ColumnSelector:
    return _dtype_selector(PySparkTypes.NUMERIC.value)


def temporal() -> ColumnSelector:
    return _dtype_selector(PySparkTypes.TEMPORAL.value)


def date() -> ColumnSelector:
    return _dtype_selector(T.DateType)


def time() -> ColumnSelector:
    return _dtype_selector((T.TimestampType, T.TimestampNTZType))


def interval() -> ColumnSelector:
    return _dtype_selector(PySparkTypes.INTERVAL.value)


def complex() -> ColumnSelector:
    return _dtype_selector(PySparkTypes.COMPLEX.value)


def required() -> ColumnSelector:
    def closure(sf: T.StructField) -> bool:
        return not sf.nullable

    return ColumnSelector(expression=closure)


def matches(pattern: str):
    """Selector capturing column names matching the pattern specified."""
    return _name_selector(
        pattern=re.compile(pattern),
        match_func=lambda name, pattern: re.search(
            re.compile(pattern), name
        ),  # swap order of parameters for _name_selector.closure
    )


def contains(pattern: str):
    """Selector capturing column names containing the exact pattern specified."""
    return _name_selector(pattern=pattern, match_func=str.__contains__)


def starts_with(pattern: str):
    """Selector capturing column names starting with the exact pattern specified."""
    return _name_selector(pattern=pattern, match_func=str.startswith)


def ends_with(pattern: str):
    """Selector capturing column names ending with the exact pattern specified."""
    return _name_selector(pattern=pattern, match_func=str.endswith)
