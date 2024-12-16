import typing

import attrs
from pyspark.sql import types as T
from tidy_tools.model._types import PYSPARK_TYPES


def get_pyspark_type(field: attrs.Attribute) -> bool:
    if isinstance(field.type, T.DataType):
        return field.type
    return PYSPARK_TYPES.get(field.type, T.NullType())


def is_optional(field: attrs.Attribute) -> bool:
    """
    Check if a field is optional.

    Parameters
    ----------
    field : attrs.Attribute
        Field defined in TidyDataModel.

    Returns
    -------
    bool
        True if field is optional; else, False.
    """
    union_type_hint = typing.get_origin(field.type) is typing.Union
    accepts_none = type(None) in typing.get_args(field.type)
    return union_type_hint and accepts_none
