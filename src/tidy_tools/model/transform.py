import typing

import attrs
from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql import types as T
from tidy_tools.model._utils import get_pyspark_type


def transform_field(field: attrs.Attribute, columns: list[str]) -> Column:
    """
    Transform data according to a class schema.

    Parameters
    ----------
    field : attrs.Attribute
        Field to apply transformation function.
    columns : list[str]
        Columns in DataFrame. Used for checking if column already exists.

    Returns
    -------
    DataFrame
        Transformed DataFrame.
    """
    if field.default:
        if isinstance(field.default, attrs.Factory):
            return_type = typing.get_type_hints(field.default.factory).get("return")
            assert (
                return_type is not None
            ), "Missing type hint for return value! Redefine function to include type hint `def func() -> pyspark.sql.Column: ...`"
            assert return_type is Column, "Factory must return a pyspark.sql.Column!"
            column = field.default.factory()
        elif field.alias not in columns:
            column = F.lit(field.default)
        else:
            column = F.when(F.col(field.alias).isNull(), field.default).otherwise(
                F.col(field.alias)
            )
    else:
        column = F.col(field.alias)

    if field.name != field.alias:
        column = column.alias(field.name)

    field_type = get_pyspark_type(field)
    match field_type:
        case T.DateType():
            column = column.cast(field_type)
        case T.TimestampType():
            column = column.cast(field_type)
        case _:
            column = column.cast(field_type)

    if field.converter:
        column = field.converter(column)

    return field
