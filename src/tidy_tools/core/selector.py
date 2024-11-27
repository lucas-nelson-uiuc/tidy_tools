from typing import Callable
from enum import Enum
import re
import operator

from attrs import define, field

from pyspark.sql import Column, types as T


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

    def __or__(self, other):
        if not isinstance(other, ColumnSelector):
            return NotImplemented
        return ColumnSelector(lambda col: self.expression(col) or other.expression(col))
    
    def __and__(self, other):
        if not isinstance(other, ColumnSelector):
            return NotImplemented
        return ColumnSelector(lambda col: self.expression(col) and other.expression(col))
    
    def __ror__(self, other):
        return self.__or__(other)
    
    def __rand__(self, other):
        return self.__and__(other)

    def __call__(self, column: str) -> bool:
        return self.expression(column)


def _name_selector(pattern: str, match_func: Callable):
    def closure(sf: T.StructField) -> bool:
        return match_func(sf.name, pattern)

    return ColumnSelector(expression=closure)


def _dtype_selector(dtypes: tuple[T.DataType]):
    def closure(sf: T.StructField) -> bool:
        return isinstance(sf.dataType, dtypes.value)

    return ColumnSelector(expression=closure)


def string():
    return _dtype_selector(PySparkTypes.STRING)


def numeric():
    return _dtype_selector(PySparkTypes.NUMERIC)


def temporal():
    return _dtype_selector(PySparkTypes.TEMPORAL)


def interval():
    return _dtype_selector(PySparkTypes.INTERVAL)


def complex():
    return _dtype_selector(PySparkTypes.COMPLEX)


def required():
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
