import inspect
import re
from enum import Enum
from typing import Callable

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

    def __repr__(self) -> str:
        """Generate a string representation using the callable's docstring."""
        try:
            doc = inspect.getdoc(self.expression)
            if doc:
                return f"ColumnSelector(expression={doc})"
            return "ColumnSelector(expression=<no docstring provided>)"
        except Exception:
            return "ColumnSelector(expression=<uninspectable callable>)"


def _name_selector(pattern: str, match_func: Callable) -> Callable:
    """
    Define Column Selector based on a Struct Field's name property.

    Parameters
    ----------
    pattern : str
        Pattern to search for in name.
    match_func : Callable
        Match function to run pattern against.

    Returns
    -------
    Callable
        Closure to filter against a Struct Field.
    """

    def closure(sf: T.StructField) -> bool:
        """
        Construct StructField filtering function.

        Parameters
        ----------
        sf : T.StructField
            PySpark StructField, often yielded in DataFrame.schema.

        Returns
        -------
        bool
            Asserts whether field's name matches pattern.
        """
        closure.__doc__ = f"Matches '{pattern}'"
        return match_func(sf.name, pattern)

    return ColumnSelector(expression=closure)


def _dtype_selector(dtype: T.DataType | tuple[T.DataType]) -> Callable:
    """
    Define Column Selector based on a Struct Field's dtype property.

    Parameters
    ----------
    dtype : T.DataType | tuple[T.DataType]
        PySpark data type.

    Returns
    -------
    Callable
        Closure to filter against a Struct Field.
    """

    def closure(sf: T.StructField) -> bool:
        """
        Construct StructField filtering function.

        Parameters
        ----------
        sf : T.StructField
            PySpark StructField, often yielded in DataFrame.schema.

        Returns
        -------
        bool
            Asserts whether field's dtype matches pattern.
        """
        closure.__doc__ = f"Data type {dtype}"
        return isinstance(sf.dataType, dtype)

    return ColumnSelector(expression=closure)


def string() -> ColumnSelector:
    """
    Select all columns with a string dtype.

    Returns
    -------
    ColumnSelector
        Predicate to filter columns.
    """
    return _dtype_selector(PySparkTypes.STRING.value)


def numeric() -> ColumnSelector:
    """
    Select all columns with a numeric dtype.

    Returns
    -------
    ColumnSelector
        Predicate to filter columns.
    """
    return _dtype_selector(PySparkTypes.NUMERIC.value)


def temporal() -> ColumnSelector:
    """
    Select all columns with a temporal dtype.

    Returns
    -------
    ColumnSelector
        Predicate to filter columns.
    """
    return _dtype_selector(PySparkTypes.TEMPORAL.value)


def date() -> ColumnSelector:
    """
    Select all columns with a date dtype.

    Returns
    -------
    ColumnSelector
        Predicate to filter columns.
    """
    return _dtype_selector(T.DateType)


def time() -> ColumnSelector:
    """
    Select all columns with a time dtype.

    Returns
    -------
    ColumnSelector
        Predicate to filter columns.
    """
    return _dtype_selector((T.TimestampType, T.TimestampNTZType))


def interval() -> ColumnSelector:
    """
    Select all columns with an interval dtype.

    Returns
    -------
    ColumnSelector
        Predicate to filter columns.
    """
    return _dtype_selector(PySparkTypes.INTERVAL.value)


def complex() -> ColumnSelector:
    """
    Select all columns with a complex dtype.

    Returns
    -------
    ColumnSelector
        Predicate to filter columns.
    """
    return _dtype_selector(PySparkTypes.COMPLEX.value)


def by_dtype(*dtype: T.DataType) -> Callable:
    """
    Select all columns with dtype(s).

    Parameters
    ----------
    *dtype : T.DataType
        One or more data types to filter for.

    Returns
    -------
    Callable
        ColumnSelector predicate filtering for `dtype`.
    """
    return _dtype_selector(dtype)


def required() -> ColumnSelector:
    """
    Return all non-nullable fields.

    Returns
    -------
    ColumnSelector
        Predicate-based column selecting function.
    """

    def closure(sf: T.StructField) -> bool:
        """
        Construct StructField filtering function.

        Parameters
        ----------
        sf : T.StructField
            PySpark StructField.

        Returns
        -------
        bool
            Asserts whether field is not nullable.
        """
        return not sf.nullable

    return ColumnSelector(expression=closure)


def exclude(*name: str) -> ColumnSelector:
    """
    Remove all columns with `name`(s).

    Parameters
    ----------
    *name : str
        Name of column(s) to exclude.

    Returns
    -------
    ColumnSelector
        ColumnSelector predciate filtering for `dtype`.
    """

    def closure(sf: T.StructField) -> bool:
        """
        Construct StructField filtering function.

        Parameters
        ----------
        sf : T.StructField
            PySpark StructField.

        Returns
        -------
        bool
            Asserts whether field is not in `name`.
        """
        return sf.name not in name

    return ColumnSelector(expression=closure)


def matches(pattern: str) -> ColumnSelector:
    """
    Selector capturing column names matching the pattern specified.

    Parameters
    ----------
    pattern : str
        Regular expression to match against a column's name.

    Returns
    -------
    ColumnSelector
        Expression filtering for column matching `pattern`.
    """
    return _name_selector(
        pattern=re.compile(pattern),
        match_func=lambda name, pattern: re.search(
            re.compile(pattern), name
        ),  # swap order of parameters for _name_selector.closure
    )


def contains(pattern: str) -> ColumnSelector:
    """
    Selector capturing column names containing the exact pattern specified.

    Parameters
    ----------
    pattern : str
        Regular expression to match against a column's name.

    Returns
    -------
    ColumnSelector
        Expression filtering for column containing `pattern`.
    """
    return _name_selector(pattern=pattern, match_func=str.__contains__)


def starts_with(pattern: str) -> ColumnSelector:
    """
    Selector capturing column names starting with the exact pattern specified.

    Parameters
    ----------
    pattern : str
        Regular expression to match against a column's name.

    Returns
    -------
    ColumnSelector
        Expression filtering for column starting with `pattern`.
    """
    return _name_selector(pattern=pattern, match_func=str.startswith)


def ends_with(pattern: str) -> ColumnSelector:
    """
    Selector capturing column names ending with the exact pattern specified.

    Parameters
    ----------
    pattern : str
        Regular expression to match against a column's name.

    Returns
    -------
    ColumnSelector
        Expression filtering for column ending with `pattern`.
    """
    return _name_selector(pattern=pattern, match_func=str.endswith)


def by_name(*name: str) -> ColumnSelector:
    """
    Selector capturing column(s) by name.

    Parameters
    ----------
    *name : str
        Name of column(s) to select.

    Returns
    -------
    ColumnSelector
        Expression filtering for columns with `name`.
    """
    return matches(pattern=rf"^({'|'.join(name)})$")
