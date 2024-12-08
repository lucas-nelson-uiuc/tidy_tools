import functools
from pathlib import Path
from typing import Callable
from typing import Optional

import attrs
from attrs import define
from attrs import field
from attrs import validators
from loguru import logger
from pyspark.errors import PySparkException
from pyspark.sql import DataFrame
from pyspark.sql import GroupedData
from tidy_tools.core import reader
from tidy_tools.core.selector import ColumnSelector
from tidy_tools.frame.context import TidyContext
from tidy_tools.frame.logger import TidyLogHandler


@define
class TidyDataFrame:
    """Enable tidy operations on a PySpark DataFrame. Context is a dictionary acting as a log."""

    _data: DataFrame = field(validator=validators.instance_of((DataFrame, GroupedData)))
    _context: Optional[dict] = field(factory=TidyContext)

    def __attrs_post_init__(self):
        if self._context.log_handlers:
            handlers = [
                attrs.asdict(handler)
                if isinstance(handler, TidyLogHandler)
                else handler
                for handler in self._context.log_handlers
            ]
            logger.configure(handlers=handlers)

    def __repr__(self):
        return (
            f"{self._context.name} [{self.count():,} rows x {len(self.columns)} cols]"
        )

    def _repr_html_(self):
        return self.__repr__()

    ## @classmethod
    # def register(cls, module):
    #     """Register external functions as methods of TidyDataFrame."""
    #     for name, func in inspect.getmembers(module, inspect.isfunction):
    #         setattr(cls, name, func)

    def _snapshot(self, operation: str, message: str, dimensions: tuple[int, int]):
        """Captures a snapshot of the DataFrame"""
        # snapshot = TidySnapshot(
        #     operation=operation,
        #     message=message,
        #     schema=self._data.schema,
        #     dimensions=dimensions,
        # )
        # self._context.log.append(snapshot)
        pass

    def _log(
        self,
        operation: str = "comment",
        message: str = "no message provided",
        level: str = "success",
    ) -> None:
        getattr(logger, level)(f"#> {operation:<12}: {message}")
        return self

    def _record(message: str, alias: Optional[str] = None) -> None:
        def decorator(func: Callable):
            @functools.wraps(func)
            def wrapper(self, *args, **kwargs):
                if hasattr(self, func.__name__):
                    # generate result of calling method on data
                    result = func(self, *args, **kwargs)

                    # log message to logging handler(s)
                    description = kwargs.get("description", "")
                    # if self._context.log is not None:
                    #     self._snapshot(
                    #         operation=alias or func.__name__,
                    #         message = eval(f"f'{message} ({description})'").strip().replace(" ()", ""),
                    #         dimensions=(self.count(), len(self._data.columns)),
                    #     )
                    self._log(
                        operation=alias or func.__name__,
                        # message=eval(f"f'{message}'") + f" - {kwargs.get('description')}",
                        message=eval(f"f'{message} ({description})'")
                        .strip()
                        .replace(" ()", ""),
                    )
                return result

            return wrapper

        return decorator

    @classmethod
    def from_source(
        cls,
        *source: str | Path | DataFrame,
        context: Optional[TidyContext] = None,
        read_func: Optional[Callable] = None,
        **read_options: dict,
    ) -> "TidyDataFrame":
        try:
            read_func = functools.partial(read_func, **read_options)
            data = reader.read(source, read_func=read_func)
            if context:
                return TidyDataFrame(data, context)
            return TidyDataFrame(data)
        except PySparkException as e:
            raise e

    @property
    def columns(self):
        """Returns the raw Spark DataFrame"""
        return self._data.columns

    @property
    def dtypes(self):
        """Return all column names and data types as a list"""
        return self._data.dtypes

    @property
    def describe(self, *cols):
        """Compute basic statistics for numeric and string columns."""
        return self._data.describe(*cols)

    @property
    def schema(self):
        return self._data.schema

    @property
    def data(self):
        """Returns the raw Spark DataFrame"""
        logger.info(">> exit: TidyDataFrame context ending.")
        return self._data

    def display(self, limit: Optional[int] = None):
        """
        Control execution of display method

        This method masks the `pyspark.sql.DataFrame.display` method. This method does not
        mask the native PySpark display function.

        Often, the `.display()` method will need to be disabled for logging purposes. Similar
        to toggling the `.count()` method, users can temporarily disable a DataFrame's
        ability to display to the console by passing `toggle_display = True`.
        """
        if not self._context.display:
            self._log(
                operation="display", message="display is toggled off", level="warning"
            )
        else:
            self._data.limit(limit or self._context.limit).display()
        return self

    def show(self, limit: Optional[int] = None):
        """
        Control execution of display method

        This method masks the `pyspark.sql.DataFrame.display` method. This method does not
        mask the native PySpark display function.

        Often, the `.display()` method will need to be disabled for logging purposes. Similar
        to toggling the `.count()` method, users can temporarily disable a DataFrame's
        ability to display to the console by passing `toggle_display = True`.
        """
        if not self._context.display:
            self._log(
                operation="display", message="display is toggled off", level="warning"
            )
        else:
            self._data.limit(limit or self._context.limit).show()
        return self

    def count(self, result: Optional[DataFrame] = None) -> int:
        """Retrieve number of rows in DataFrame."""
        if not self._context.count:
            return 0
        # if not self._context.log:
        #     return self._data.count()
        if result:
            return result._data.count()
        return self._data.count()

    @_record(message="selected {len(result._data.columns)} columns")
    def select(
        self,
        *selectors: ColumnSelector,
        strict: bool = True,
        invert: bool = False,
        description: Optional[str] = None,
    ):
        compare_operator = all if strict else any
        selected = set(
            [
                field.name
                for field in self.schema
                if compare_operator(
                    selector.expression(field) for selector in selectors
                )
            ]
        )
        if invert:
            result = self._data.drop(*selected)
        else:
            result = self._data.select(*selected)
        return TidyDataFrame(result, self._context)

    def drop(
        self,
        *selectors: ColumnSelector,
        strict: bool = True,
        description: Optional[str] = None,
    ) -> "TidyDataFrame":
        return self.select(*selectors, strict=strict, invert=True)

    @_record(message="removed {self.count() - self.count(result):,} rows")
    def filter(self, condition, description: Optional[str] = None):
        result = self._data.filter(condition)
        return TidyDataFrame(result, self._context)

    @_record(
        message='added column {args[0] if args else kwargs.get("colName")}',
        alias="mutate",
    )
    def withColumn(self, colName, col, description: Optional[str] = None):
        result = self._data.withColumn(colName, col)
        return TidyDataFrame(result, self._context)

    def transform(self, func: Callable, *args, **kwargs):
        """Concise syntax for chaining custom transformations together."""
        result = func(self, *args, **kwargs)
        return TidyDataFrame(result._data, self._context)

    def pipe(self, *funcs: Callable):
        """Chain multiple custom transformation functions to be applied iteratively."""
        result = functools.reduce(lambda init, func: init.transform(func), funcs, self)

        return TidyDataFrame(result._data, self._context)

    def __getattr__(self, attr):
        """
        Override default getattr 'dunder' method.

        TidyDataFrame will (most likely) never cover all pyspark.sql.DataFrame
        methods for many reasons. However, it still offers users the chance to
        make use of these methods as if they were calling it from a DataFrame.
        This function will evaluate if and only if an attribute is not available
        in TidyDataFrame.

        If the attribute is available in pyspark.sql.DataFrame, the result will
        be calculated and returned as a TidyDataFrame. This is to allow the user
        to continue receiving logging messages on methods (if any) called after
        said attribute.

        If the attribute is not available in pyspark.sql.DataFrame, the
        corresponding pyspark error will be raised.
        """
        if hasattr(self._data, attr):

            def wrapper(*args, **kwargs):
                result = getattr(self._data, attr)(*args, **kwargs)
                if isinstance(result, DataFrame):
                    self._log(
                        operation=attr, message="not yet implemented", level="warning"
                    )
                    return TidyDataFrame(result, self._context)
                else:
                    return self

            return wrapper
        ### TODO: validate if this logging operation is legit
        ### TODO: mark as unstable (sometimes get notebook dependencies caught in this; generates long message)
        # self._log(operation=attr, message="method does not exist", level="error")
        raise AttributeError(
            f"'{type(self._data).__name__}' object has no attribute '{attr}'"
        )
