import functools
import operator
from typing import Callable

import attrs
from pyspark.sql import Column
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from tidy_tools.error import TidyError


def _mapper(validator: Callable) -> Column:
    """
    Decompose attrs validator patterns into native Python expressions to be executed in PySpark.

    Parameters
    ----------
    validator : attrs._ValidatorType
        One of attrs built-in validators. See `attrs.validator` for details.

    Returns
    -------
    Column
        Expression(s) to execute in PySpark context.
    """
    match validator.__class__.__name__:
        case "_NumberValidator":
            return lambda name: validator.compare_func(F.col(name), validator.bound)
        case "_InValidator":
            return lambda name: F.col(name).isin(validator.options)
        case "_MatchesReValidator":
            return lambda name: F.col(name).rlike(validator.pattern.pattern)
        case "_MinLengthValidator":
            return lambda name: operator.ge(F.length(F.col(name)), validator.min_length)
        case "_MaxLengthValidator":
            return lambda name: operator.le(F.length(F.col(name)), validator.max_length)
        case "_OrValidator":
            return lambda name: functools.reduce(
                operator.or_,
                map(lambda v: _mapper(v)(name=name), validator._validators),
            )
        case "_AndValidator":
            return lambda name: functools.reduce(
                operator.and_,
                map(lambda v: _mapper(v)(name=name), validator._validators),
            )
        case _:  # assumes validator is user-defined function
            return lambda name: validator(name)


def validate_field(cls_field: attrs.Attribute, data: DataFrame) -> TidyError:
    """
    Apply validation function(s) to schema cls_field.

    Parameters
    ----------
    cls_field : attrs.Attribute
        Schema for field in class.
    data : DataFrame
        Data to validate field against.

    Returns
    -------
    TidyError
        If the validation function fails for at least one row, an error handler
        is returned for further logging.
    """
    validate_func = _mapper(cls_field.validator)
    # TODO: add support for TidyDataFrame;
    # should disable messages to avoid unnecessary filter messages
    invalid_entries = data.filter(operator.inv(validate_func(cls_field.name)))
    try:
        assert invalid_entries.isEmpty()
        error = None
    except AssertionError:
        error = TidyError(cls_field.name, validate_func, invalid_entries)
    finally:
        return error
