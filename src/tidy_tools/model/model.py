import datetime
import decimal
import functools
import operator
import sys
import typing
from collections import deque
from types import MappingProxyType
from typing import Callable
from typing import Iterable

import attrs
from attrs import define
from loguru import logger
from pyspark.sql import Column
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from tidy_tools.model.error import TidyError

logger.remove()
logger.add(sys.stderr, format="{time:HH:mm:ss} | <level>{level:<8}</level> | {message}")


PYSPARK_TYPES = MappingProxyType(
    {
        str: T.StringType(),
        int: T.IntegerType(),
        float: T.FloatType(),
        decimal.Decimal: T.DecimalType(38, 6),
        datetime.date: T.DateType(),
        datetime.datetime: T.TimestampType(),
    }
)


def get_pyspark_type(field: attrs.Attribute) -> bool:
    if isinstance(field.type, T.DataType):
        return field.type
    return PYSPARK_TYPES.get(field.type, T.NullType())


def is_optional(field: attrs.Attribute) -> bool:
    """Check if a field is optional"""
    union_type_hint = typing.get_origin(field.type) is typing.Union
    accepts_none = type(None) in typing.get_args(field.type)
    return union_type_hint and accepts_none


@define
class TidyDataModel:
    @classmethod
    def __attrs_init_subclass__(cls):
        logger.info(f"{cls.__name__} was created using TidyDataModel as reference.")

    @classmethod
    def schema(cls, coerce_types: bool = False) -> T.StructType:
        return T.StructType(
            [
                T.StructField(
                    field.name,
                    get_pyspark_type(field) if coerce_types else T.StringType(),
                    is_optional(field),
                )
                for field in attrs.fields(cls)
            ]
        )

    @classmethod
    def required_fields(cls) -> Iterable[str]:
        return [field for field in attrs.fields(cls) if not is_optional(field)]

    @classmethod
    def _read(cls, func: Callable, *args, **kwargs):
        return functools.partial(func.schema(cls.schema()).csv, *args, **kwargs)

    @classmethod
    def read(
        cls,
        *source: Iterable[str],
        read_options: dict = dict(),
        union_func: Callable = DataFrame.unionByName,
    ) -> DataFrame:
        cls.document("_source", source)
        read_func = cls._read(**read_options)
        return functools.reduce(union_func, map(read_func, source))

    @classmethod
    def transform(
        cls,
        data: DataFrame,
        preprocess: Callable = None,
        postprocess: Callable = None,
    ):
        queue = deque()

        for field in attrs.fields(cls):
            if field.default:
                if isinstance(field.default, attrs.Factory):
                    return_type = typing.get_type_hints(field.default.factory).get(
                        "return"
                    )
                    assert (
                        return_type is not None
                    ), "Missing type hint for return value! Redefine function to include type hint `def func() -> pyspark.sql.Column: ...`"
                    assert (
                        return_type is Column
                    ), "Factory must return a pyspark.sql.Column!"
                    column = field.default.factory()
                elif field.alias not in data.columns:
                    column = F.lit(field.default)
                else:
                    column = F.when(
                        F.col(field.alias).isNull(), field.default
                    ).otherwise(F.col(field.alias))
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

            queue.append(column)
            cls.document("_transformations", {field.name: column})

        if preprocess:
            if hasattr(cls, "__tidy_preprocess__"):
                logger.warn("Preprocess function already defined!")
            data = preprocess(data)

        data = data.withColumns(
            {field.name: column for field, column in zip(attrs.fields(cls), queue)}
        )

        if postprocess:
            if hasattr(cls, "__tidy_postprocess__"):
                logger.warn("Postprocess function already defined!")
            data = postprocess(data)

        return data

    @classmethod
    def _validate(cls, validator) -> Column:
        match validator.__class__.__name__:
            case "_NumberValidator":
                return lambda name: validator.compare_func(F.col(name), validator.bound)
            case "_InValidator":
                return lambda name: F.col(name).isin(validator.options)
            case "_MatchesReValidator":
                return lambda name: F.col(name).rlike(validator.pattern)
            case "_MinLengthValidator":
                return lambda name: operator.ge(
                    F.length(F.col(name)), validator.min_length
                )
            case "_MaxLengthValidator":
                return lambda name: operator.le(
                    F.length(F.col(name)), validator.max_length
                )
            case "_OrValidator":
                return lambda name: functools.reduce(
                    operator.or_,
                    map(lambda v: cls._validate(v)(name=name), validator._validators),
                )
            case "_AndValidator":
                return lambda name: functools.reduce(
                    operator.and_,
                    map(lambda v: cls._validate(v)(name=name), validator._validators),
                )

    @classmethod
    def validate(cls, data: DataFrame):
        errors = deque()
        for field in attrs.fields(cls):
            if field.validator:
                validate_func = cls._validate(field.validator)
                invalid_entries = data.filter(operator.inv(validate_func(field.name)))
                if not invalid_entries.isEmpty():
                    logger.error(
                        f"{field.name} failed {invalid_entries.count():,} ({invalid_entries.count() / data.count():.1%}) rows"
                    )
                    errors.append(TidyError(field.name, validate_func, invalid_entries))
                else:
                    logger.success(f"All validation(s) passed for `{field.name}`")
                cls.document(
                    "_validations", {field.name: validate_func}
                )  # TODO: move documentation into cls._validate
        cls.document("_errors", errors)
        return data

    @classmethod
    def pipe(cls, data: DataFrame) -> DataFrame:
        return cls.validate(cls.transform(data=data))

    @classmethod
    def show_errors(
        cls, summarize: bool = False, limit: int = 10, export: bool = False
    ) -> None:
        if not hasattr(cls, "_errors"):
            logger.warning(
                f"{cls.__name__} has not yet defined `_errors`. Please run {cls.__name__}.validate(<data>) or {cls.__name__}.pipe(<data>)."
            )
            return

        errors = getattr(cls, "_errors")
        if not errors:
            logger.success(f"{cls.__name__} has no errors!")
        for error in errors:
            logger.info(
                f"Displaying {limit:,} of {error.data.count():,} rows that do not meet the following validation(s): {error.validation(error.column)}"
            )
            data = (
                error.data.groupby(error.column).count().orderBy(F.col("count").desc())
                if summarize
                else error.data
            )
            data.limit(limit).show()

    @classmethod
    def document(cls, attribute, value) -> dict:
        if hasattr(cls, attribute):
            attr = getattr(cls, attribute)
            if isinstance(value, dict):
                value |= attr
        setattr(cls, attribute, value)

    @classmethod
    @property
    def documentation(cls) -> dict:
        # return cls._documentation
        return {
            "name": cls.__name__,
            "description": cls.__doc__,
            "sources": cls._source,
            "transformations": cls._transformations,
            "validations": cls._validations,
            "fields": attrs.fields(cls),
        }

    @classmethod
    def format_mapping(cls) -> dict:
        def format_validation(
            field: attrs.Attribute, validations: dict[str, Callable]
        ) -> str:
            if field.name not in validations:
                return "No user-defined validations."
            return validations.get(field.name)(field.name)

        def format_transformation(
            field: attrs.Attribute, transformations: dict[str, Callable]
        ) -> str:
            if field.name not in transformations:
                return "No user-defined validations."
            return transformations.get(field.name)

        validations = cls.documentation.get("validations")
        transformations = cls.documentation.get("transformations")

        return [
            {
                "Field Name": field.name,
                "Field Description": field.metadata.get(
                    "description", "No description provided"
                ),
                "Field Type": field.type.__name__,
                "Mapping": field.alias,
                "Validations": format_validation(field, validations),
                "Transformations": format_transformation(field, transformations),
            }
            for field in cls.documentation.get("fields")
        ]
