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
from tidy_tools.model.convert import convert_field
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
                    cls_field.name,
                    get_pyspark_type(cls_field) if coerce_types else T.StringType(),
                    is_optional(cls_field),
                )
                for cls_field in attrs.fields(cls)
            ]
        )

    @classmethod
    def required_fields(cls) -> Iterable[str]:
        return [
            cls_field for cls_field in attrs.fields(cls) if not is_optional(cls_field)
        ]

    @classmethod
    def __preprocess__(cls, data: DataFrame) -> DataFrame:
        """
        Optional function to apply to data before conversion and validation.

        Parameters
        ----------
        data : DataFrame
            Object to apply function to.

        Returns
        -------
        DataFrame
            Converted DataFrame.
        """
        return data

    @classmethod
    def __postprocess__(cls, data: DataFrame) -> DataFrame:
        """
        Optional function to apply to data after conversion and validation.

        Parameters
        ----------
        data : DataFrame
            Object to apply function to.

        Returns
        -------
        DataFrame
            Converted DataFrame.
        """
        return data

    @classmethod
    def load(
        cls,
        *source: str,
        read_func: Callable,
        read_options: dict = dict(),
    ) -> DataFrame:
        """
        Load data from source(s) and apply processing, conversion, and validation procedures.

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
        read_func = functools.partial(read_func, schema=cls.schema(), **read_options)
        data = reader.read(*source, read_func=read_func)
        process = cls.tidy()
        return process(data)  # TODO: add option to use TidyDataFrame

    @classmethod
    def convert(cls, data: DataFrame):
        """
        Apply conversion functions to supported fields.

        Outputs messages to logging handlers.

        Parameters
        ----------
        data : DataFrame
            Object to apply conversion functions.

        Returns
        -------
        DataFrame
            Converted data.
        """
        queue = {
            cls_field: convert_field(
                cls_field=cls_field, cls_field_exists=cls_field.alias in data.columns
            )
            for cls_field in attrs.fields(cls)
        }

        # return data.withColumns(
        #     {cls_field.name: column for cls_field, column in queue.items()}
        # )
        return data.select(*(column for column in queue.values()))

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
            cls_field: validate_field(cls_field, data=data)
            for cls_field in attrs.fields(cls)
            if cls_field.validator
        }

        n_rows = data.count()
        for cls_field, error in errors.items():
            if error is not None:
                n_failures = error.data.count()
                logger.error(
                    f"Validation(s) failed for `{cls_field.name}`: {n_failures:,} rows ({n_failures / n_rows:.1%})"
                )
            else:
                logger.success(f"All validation(s) passed for `{cls_field.name}`")
        return data

    @classmethod
    def tidy(cls) -> Callable:
        """
        Method for composing processing functions.

        If present, the methods are executed in the following order:
            - pre-processing
            - conversions
            - validations
            - post-processing

        Returns
        -------
        Callable
            Function to call listed methods.
        """
        return compose(
            cls.__preprocess__, cls.convert, cls.validate, cls.__postprocess__
        )

    @classmethod
    def transform(cls, data: DataFrame) -> DataFrame:
        return data

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
