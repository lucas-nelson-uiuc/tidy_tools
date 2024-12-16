import typing

import attrs
from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql import types as T
from tidy_tools.model._utils import get_pyspark_type


def transform_field(cls_field: attrs.Attribute, cls_field_exists: bool) -> Column:
    """
    Transform data according to a class schema.

    Parameters
    ----------
    cls_field : attrs.Attribute
        Field to apply transformation function.
    cls_field_exists : bool
        Whether field exists in data already.

    Returns
    -------
    DataFrame
        Transformed DataFrame.
    """
    if cls_field.default:
        if isinstance(cls_field.default, attrs.Factory):
            return_type = typing.get_type_hints(cls_field.default.factory).get("return")
            assert (
                return_type is not None
            ), "Missing type hint for return value! Redefine function to include type hint `def func() -> pyspark.sql.Column: ...`"
            assert return_type is Column, "Factory must return a pyspark.sql.Column!"
            column = cls_field.default.factory()
        elif not cls_field_exists:
            column = F.lit(cls_field.default)
        else:
            column = F.when(
                F.col(cls_field.alias).isNull(), cls_field.default
            ).otherwise(F.col(cls_field.alias))
    else:
        column = F.col(cls_field.alias)

    if cls_field.name != cls_field.alias:
        column = column.alias(cls_field.name)

    cls_field_type = get_pyspark_type(cls_field)
    match cls_field_type:
        case T.DateType():
            column = column.cast(cls_field_type)
        case T.TimestampType():
            column = column.cast(cls_field_type)
        case _:
            column = column.cast(cls_field_type)

    if cls_field.converter:
        column = cls_field.converter(column)

    return column
