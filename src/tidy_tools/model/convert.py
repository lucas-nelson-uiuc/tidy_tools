import typing

import attrs
from loguru import logger
from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql import types as T
from tidy_tools.model._utils import get_pyspark_type


def convert_field(cls_field: attrs.Attribute, cls_field_exists: bool) -> Column:
    """
    Convert data according to a class schema.

    Parameters
    ----------
    cls_field : attrs.Attribute
        Field to apply conversion function.
    cls_field_exists : bool
        Whether field exists in data already.

    Returns
    -------
    DataFrame
        Converted DataFrame.
    """
    if not cls_field.default:
        if not cls_field_exists:
            column = F.lit(None).alias(cls_field.alias)
        else:
            column = F.col(cls_field.alias)

    if cls_field.default:
        if isinstance(cls_field.default, attrs.Factory):
            return_type = typing.get_type_hints(cls_field.default.factory).get("return")
            assert (
                return_type is not None
            ), "Missing type hint for return value! Redefine function to include type hint `def func() -> pyspark.sql.Column: ...`"
            assert return_type is Column, "Factory must return a pyspark.sql.Column!"
            column = cls_field.default.factory()
        elif not cls_field_exists:
            column = F.lit(cls_field.default).alias(cls_field.alias)
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
            date_format = cls_field.metadata.get("format")
            if date_format:
                column = F.to_date(column, format=date_format)
            else:
                logger.warning(
                    f"No `format` provided for {cls_field.name}. Please add `field(..., metadata={{'format': ...}}) and rerun."
                )
                column = column.cast(cls_field_type)
        case T.TimestampType():
            timestamp_format = cls_field.metadata.get("format")
            if timestamp_format:
                column = F.to_timestamp(column, format=timestamp_format)
            else:
                logger.warning(
                    f"No `format` provided for {cls_field.name}. Please add `field(..., metadata={{'format': ...}}) and rerun."
                )
                column = column.cast(cls_field_type)

    if cls_field.converter:
        column = cls_field.converter(column)

    return column
