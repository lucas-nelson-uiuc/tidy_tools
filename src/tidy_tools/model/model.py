import functools
from typing import Callable
from typing import Iterable

import attrs
from attrs import define
from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from tidy_tools.functions import reader
from tidy_tools.model._utils import get_pyspark_type
from tidy_tools.model._utils import is_optional
from tidy_tools.model.transform import transform_field
from tidy_tools.model.validate import validate_field
from tidy_tools.workflow.pipeline import compose


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
    def __preprocess__(cls, data: DataFrame) -> DataFrame:
        """
        Optional function to apply to data before transformation and validation.

        Parameters
        ----------
        data : DataFrame
            Object to apply function to.

        Returns
        -------
        DataFrame
            Transformed DataFrame.
        """
        return data

    @classmethod
    def __postprocess__(cls, data: DataFrame) -> DataFrame:
        """
        Optional function to apply to data after transformation and validation.

        Parameters
        ----------
        data : DataFrame
            Object to apply function to.

        Returns
        -------
        DataFrame
            Transformed DataFrame.
        """
        return data

    @classmethod
    def read(
        cls,
        *source: str,
        read_func: Callable,
        read_options: dict = dict(),
    ) -> DataFrame:
        """
        Load data from source(s) and apply processing, transformation, and validation procedures.

        See `TidyDataModel.tidy()` for more details.

        Parameters
        ----------
        *source : str
            Arbitrary number of reference(s) to data source(s).
        read_func : Callable
            Function to load data from source(s).
        read_options : dict
            Keyword arguments to pass to `read_func`.

        Returns
        -------
        DataFrame
            Single DataFrame containing data from all source(s) coerced according to class schema.
        """
        cls.document("_source", source)
        read_func = functools.partial(read_func, schema=cls.schema(), **read_options)
        data = reader.read(*source, read_func=read_func)
        process = cls.tidy()
        return process(data)

    @classmethod
    def transform(cls, data: DataFrame):
        """
        Apply transformation functions to supported fields.

        Outputs messages to logging handlers.

        Parameters
        ----------
        data : DataFrame
            Object to apply transformation functions.

        Returns
        -------
        DataFrame
            Transformed data.
        """
        queue = {
            field.name: transform_field(field, columns=data.columns)
            for field in attrs.fields(cls)
        }

        # queue = deque()

        # for field in attrs.fields(cls):
        #     if field.default:
        #         if isinstance(field.default, attrs.Factory):
        #             return_type = typing.get_type_hints(field.default.factory).get(
        #                 "return"
        #             )
        #             assert (
        #                 return_type is not None
        #             ), "Missing type hint for return value! Redefine function to include type hint `def func() -> pyspark.sql.Column: ...`"
        #             assert (
        #                 return_type is Column
        #             ), "Factory must return a pyspark.sql.Column!"
        #             column = field.default.factory()
        #         elif field.alias not in data.columns:
        #             column = F.lit(field.default)
        #         else:
        #             column = F.when(
        #                 F.col(field.alias).isNull(), field.default
        #             ).otherwise(F.col(field.alias))
        #     else:
        #         column = F.col(field.alias)

        #     if field.name != field.alias:
        #         column = column.alias(field.name)

        #     field_type = get_pyspark_type(field)
        #     match field_type:
        #         case T.DateType():
        #             column = column.cast(field_type)
        #         case T.TimestampType():
        #             column = column.cast(field_type)
        #         case _:
        #             column = column.cast(field_type)

        #     if field.converter:
        #         column = field.converter(column)

        #     queue.append(column)
        #     cls.document("_transformations", {field.name: column})

        return data.withColumns({field: column for field, column in queue.items()})

    @classmethod
    def validate(cls, data: DataFrame) -> DataFrame:
        """
        Apply validation functions to supported fields.

        Outputs messages to logging handlers.

        Parameters
        ----------
        data : DataFrame
            Object to apply validations functions.

        Returns
        -------
        DataFrame
            Original data passed to function.
        """
        errors = {
            field.name: validate_field(field, data=data)
            for field in attrs.fields(cls)
            if field.validator
        }

        n_rows = data.count()
        for field, error in errors.items():
            if error is not None:
                n_failures = error.data.count()
                logger.error(
                    f"Validation(s) failed for `{field.name}`: {n_failures:,} rows ({n_failures / n_rows:.1%})"
                )
            else:
                logger.success(f"All validation(s) passed for `{field.name}`")
        return data

    @classmethod
    def tidy(cls) -> Callable:
        """
        Method for composing processing functions.

        If present, the methods are executed in the following order:
            - pre-processing
            - transformations
            - validations
            - post-processing

        Returns
        -------
        Callable
            Function to call listed methods.
        """
        return compose(
            cls.__preprocess__, cls.transform, cls.validate, cls.__postprocess__
        )

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
