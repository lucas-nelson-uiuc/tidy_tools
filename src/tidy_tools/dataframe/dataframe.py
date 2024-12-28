import functools
import inspect
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
from tidy_tools.core.selector import ColumnSelector
from tidy_tools.dataframe.context import TidyContext
from tidy_tools.dataframe.handler import TidyLogHandler
from tidy_tools.functions import reader


@define
class TidyDataFrame:
    """
    Enable tidy operations on a PySpark DataFrame with optional context.

    TidyDataFrame is a PySpark DataFrame with built-in logging functionality.
    Directly inspired by the [tidylog](https://github.com/elbersb/tidylog)
    project, TidyDataFrame decorates common DataFrame methods to detail the
    impact of said method in real-time. Combined with the context to control
    other behavior (e.g. disabling displays, logging to multiple handlers),
    TidyDataFrame is the all-in-one logging solution for PySpark workflows.

    Attributes
    ----------
    _data : DataFrame
        PySpark DataFrame object to perform tidy operations.
    _context : dict, optional
        Context to control execution of TidyDataFrame.
    """

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

    def _log(
        self,
        operation: str = "comment",
        message: str = "no message provided",
        level: str = "info",
    ) -> None:
        """
        Log message to handler(s).

        Parameters
        ----------
        operation : str
            Name of operation (e.g. function).
        message : str
            Text to describe operation.
        level : str
            Logging level to register message. Must be one of the levels recognized by `loguru.logger`.

        Returns
        -------
        None
            No output returned since message is logged to handler(s).
        """
        getattr(logger, level)(f"#> {operation}: {message}")
        return self

    def _record(message: str, alias: Optional[str] = None) -> None:
        def decorator(func: Callable):
            @functools.wraps(func)
            def wrapper(self, *args, **kwargs):
                if hasattr(self, func.__name__):
                    # generate result of calling method on data
                    result = func(self, *args, **kwargs)

                    # log message to logging handler(s)
                    self._log(
                        operation=alias or func.__name__, message=eval(f"f'{message}'")
                    )
                return result

            return wrapper

        return decorator

    @classmethod
    def load(
        cls,
        *source: str | Path,
        context: Optional[TidyContext] = None,
        read_func: Optional[Callable] = None,
        read_options: Optional[dict] = dict(),
    ) -> "TidyDataFrame":
        """
        Create TidyDataFrame directly from source(s).

        Parameters
        ----------
        *source : str | Path
            Arbitrary number of file references containing data.
        context : TidyContext, optional
            Additional context parameters to pass.
        read_func : Callable, optional
            Function for reading data from source(s).
        read_options : dict, optional
            Additional parameters to pass to `read_func`.

        Returns
        -------
        TidyDataFrame
            Instance of TidyDataFrame loaded from source(s) with additional
            parameters instructing read-in and/or context.

        Examples
        --------
        >>> # load data from a single source
        >>> tidy_data = TidyDataFrame.load("path/to/data.csv")
        >>>
        >>> # load data from multiple sources
        >>> tidy_data = TidyDataFrame.load(
        >>>     "path/to/data.csv",
        >>>     "path/to/another/file.txt",
        >>>     "path/to/the/final/file.xlsx",
        >>> )
        >>>
        >>> # load data with context
        >>> tidy_data = TidyDataFrame.load(..., context=TidyContext(...))
        >>>
        >>> # load data with read-in instructions
        >>> tidy_data = TidyDataFrame.load(
        >>>     ...,
        >>>     read_func=spark.read.csv,
        >>>     read_options={"header": "true"}
        >>> )
        """
        try:
            read_func = functools.partial(read_func, **read_options)
            data = reader.read(*source, read_func=read_func)
            if context:
                return TidyDataFrame(data, context)
            return TidyDataFrame(data)
        except PySparkException as e:
            raise e

    @property
    def columns(self):  # numpydoc ignore=RT01
        """Return the raw Spark DataFrame."""
        return self._data.columns

    @property
    def dtypes(self):  # numpydoc ignore=RT01
        """Return all column names and data types as a list."""
        return self._data.dtypes

    @property
    def describe(self, *cols):  # numpydoc ignore=PR01,RT01
        """Compute basic statistics for numeric and string columns."""
        return self._data.describe(*cols)

    @property
    def schema(self):  # numpydoc ignore=RT01
        """Return schema as a pyspark.sql.types.StructType object."""
        return self._data.schema

    @property
    def data(self):  # numpydoc ignore=RT01
        """Return the raw Spark DataFrame."""
        self._log(operation="exit", message=self.__repr__())
        return self._data

    def is_empty(self):  # numpydoc ignore=RT01
        """Check if data is empty."""
        return self._data.isEmpty()

    def isEmpty(self):
        return self.is_empty()

    def display(self, limit: Optional[int] = None) -> None:
        """
        Control execution of display method.

        This method masks the `pyspark.sql.DataFrame.display` method. This method does not
        mask the native PySpark display function.

        Often, the `.display()` method will need to be disabled for logging purposes. Similar
        to toggling the `.count()` method, users can temporarily disable a DataFrame's
        ability to display to the console by passing `toggle_display = True`.

        Parameters
        ----------
        limit : int
            Number of rows to display to console. If context is provided, the limit provided
            will be used.

        Returns
        -------
        None
            Displays data to console or nothing if display is disabled.
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
        Control execution of display method.

        This method masks the `pyspark.sql.DataFrame.display` method. This method does not
        mask the native PySpark display function.

        Often, the `.display()` method will need to be disabled for logging purposes. Similar
        to toggling the `.count()` method, users can temporarily disable a DataFrame's
        ability to display to the console by passing `toggle_display = True`.

        Parameters
        ----------
        limit : int
            Number of rows to display to console. If context is provided, the limit provided
            will be used.

        Returns
        -------
        None
            Displays data to console or nothing if display is disabled.
        """
        if not self._context.display:
            self._log(
                operation="display", message="display is toggled off", level="warning"
            )
        else:
            self._data.limit(limit or self._context.limit).show()
        return self

    def count(self, result: Optional[DataFrame] = None) -> int:
        """
        Return number of rows in DataFrame.

        Parameters
        ----------
        result : DataFrame, optional
            If provided, this will trigger a count operation. Else, the count will reference
            the last count or zero if context disables count.

        Returns
        -------
        int
            Number of rows in data or zero if count is disabled in context.
        """
        if not self._context.count:
            return 0
        if result:
            return result._data.count()
        return self._data.count()

    @_record(message="selected {len(result._data.columns)} columns")
    def select(
        self, *selectors: ColumnSelector, strict: bool = True, invert: bool = False
    ) -> "TidyDataFrame":
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
    ) -> "TidyDataFrame":
        return self.select(*selectors, strict=strict, invert=True)

    @_record(message="removed {self.count() - self.count(result):,} rows")
    def filter(self, condition) -> "TidyDataFrame":
        result = self._data.filter(condition)
        return TidyDataFrame(result, self._context)

    @_record(
        message='{"edited" if (args[0] if args else kwargs.get("colName")) in self._data.columns else "added"} `{args[0] if args else kwargs.get("colName")}` (type: {dict(result.dtypes).get(args[0] if args else kwargs.get("colName"))})',
        alias="mutate",
    )
    def with_column(self, colName, col) -> "TidyDataFrame":
        result = self._data.withColumn(colName, col)
        return TidyDataFrame(result, self._context)

    def withColumn(self, colName, col) -> "TidyDataFrame":
        return self.with_column(colName, col)

    def with_columns(self, colsMap: dict) -> "TidyDataFrame":
        return functools.reduce(
            lambda init, params: init.with_column(params[0], params[1]),
            colsMap.items(),
            self,
        )

    def withColumns(self, colsMap: dict) -> "TidyDataFrame":
        return self.with_columns(colsMap)

    @_record(
        message='renamed `{args[0] if args else kwargs.get("existing")}` to `{args[1] if args else kwargs.get("new")}`',
        alias="rename",
    )
    def rename(self, existing: str, new: str) -> "TidyDataFrame":
        result = self._data.withColumnRenamed(existing, new)
        return TidyDataFrame(result, self._context)

    def withColumnRenamed(self, existing, new) -> "TidyDataFrame":
        return self.rename(existing, new)

    def withColumnsRenamed(self, colsMap: dict) -> "TidyDataFrame":
        return functools.reduce(
            lambda init, params: init.rename(params[0], params[1]),
            colsMap.items(),
            self,
        )

    def transform(self, func: Callable, *args, **kwargs) -> "TidyDataFrame":
        """
        Concise syntax for chaining custom transformations together.

        If calling multiple times in succession, consider using `TidyDataFrame.pipe`.

        Parameters
        ----------
        func : Callable
            Custom transformation function(s) to apply to data.
        *args : tuple
            Arbitrary number of positional arguments to pass to `func`.
        **kwargs : dict
            Arbitrary number of keyword arguments to pass to `func`.

        Returns
        -------
        TidyDataFrame
            Transformed data.
        """
        # include docstring in logs if provided
        docstring = inspect.getdoc(func)
        if docstring:
            self._log(operation=func.__name__, message=inspect.cleandoc(docstring))

        result = func(self, *args, **kwargs)
        return TidyDataFrame(result._data, self._context)

    def pipe(self, *funcs: Callable) -> "TidyDataFrame":
        """
        Iteratively apply custom transformation functions.

        Functional alias for `TidyDataFrame.transform`.

        Parameters
        ----------
        *funcs : Callable
            Custom transformation function(s) to apply to data.

        Returns
        -------
        TidyDataFrame
            Transformed data.
        """
        result = functools.reduce(lambda init, func: init.transform(func), funcs, self)
        return TidyDataFrame(result._data, self._context)

    def __getattr__(self, attr: str) -> "TidyDataFrame":
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

        Parameters
        ----------
        attr : str
            Attribute to get from TidyDataFrame or PySpark DataFrame.

        Returns
        -------
        TidyDataFrame
            Data with attribute.

        Raises
        ------
        AttributeError
            If attribute cannot be found in TidyDataFrame or PySpark DataFrame.
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
