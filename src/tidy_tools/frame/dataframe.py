import functools
import inspect
from typing import Callable
from typing import Optional

from attrs import define
from attrs import field
from attrs import validators
from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql import GroupedData
from tidy_tools.core.selector import ColumnSelector
from tidy_tools.frame.context import TidySnapshot


@define
class TidyDataFrame:
    _data: DataFrame = field(validator=validators.instance_of((DataFrame, GroupedData)))
    config: dict = field(factory=dict)
    context: Optional[dict] = field(default=None)

    def __attrs_post_init__(self):
        self.config.setdefault("name", self.__class__.__name__)
        self.config.setdefault("count", True)
        self.config.setdefault("display", True)
        self.config.setdefault("verbose", False)
        self.config.setdefault("register_tidy", True)

    def __repr__(self):
        return f"{self.config.get('name')} [{self.count():,} rows x {len(self.columns)} cols]"

    @classmethod
    def register(cls, module):
        """Register external functions as methods of TidyDataFrame."""
        for name, func in inspect.getmembers(module, inspect.isfunction):
            setattr(cls, name, func)

    def _snapshot(self, operation: str, message: str, dimensions: tuple[int, int]):
        """Captures a snapshot of the DataFrame"""
        snapshot = TidySnapshot(
            operation=operation,
            message=message,
            schema=self._data.schema,
            dimensions=dimensions,
        )
        if self.context is not None:
            self.context["snapshots"].append(snapshot)

    def _log(
        self,
        operation: str = "comment",
        message: str = "no message provided",
        level: str = "info",
    ) -> None:
        getattr(logger, level)(f"#> {operation:<12}: {message}")
        return self

    def _record(message: str) -> None:
        def decorator(func: Callable):
            @functools.wraps(func)
            def wrapper(self, *args, **kwargs):
                if hasattr(self, func.__name__):
                    result = func(self, *args, **kwargs)
                    if self.context:
                        self._snapshot(
                            operation=func.__name__,
                            message=eval(f"f'{message}'"),
                            dimensions=(self.count(), len(self._data.columns)),
                        )
                    self._log(
                        operation=func.__name__,
                        message=eval(f"f'{message}'"),
                    )
                return result

            return wrapper

        return decorator

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

    def display(self, limit: int = 10):
        """
        Control execution of display method

        This method masks the `pyspark.sql.DataFrame.display` method. This method does not
        mask the native PySpark display function.

        Often, the `.display()` method will need to be disabled for logging purposes. Similar
        to toggling the `.count()` method, users can temporarily disable a DataFrame's
        ability to display to the console by passing `toggle_display = True`.
        """
        if not self.config.get("display"):
            self._log(
                operation="display", message="display is toggled off", level="warning"
            )
        else:
            self._data.limit(limit).display()
        return self

    def show(self, limit: int = 10):
        if not self.config.get("display"):
            self._log(
                operation="show", message="display is toggled off", level="warning"
            )
        else:
            self._data.limit(limit).show()
        return self

    def count(self, result: Optional[DataFrame] = None) -> int:
        """Retrieve number of rows in DataFrame."""
        if not self.config.get("count"):
            return 0
        if not self.context["snapshots"]:
            return self._data.count()
        if result:
            return result._data.count()
        return self.context["snapshots"][-1].dimensions[0]

    @_record(message="selected {len(result._data.columns)} columns")
    def select(
        self, *selectors: ColumnSelector, strict: bool = True, invert: bool = False
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
        return TidyDataFrame(result, config=self.config, context=self.context)

    def drop(self, *selectors: ColumnSelector, strict: bool = True) -> "TidyDataFrame":
        return self.select(*selectors, strict=strict, invert=True)

    @_record(message="removed {self.count() - self.count(result):,} rows")
    def filter(self, condition):
        result = self._data.filter(condition)
        return TidyDataFrame(result, config=self.config, context=self.context)

    @_record(message='added column {args[0] if args else kwargs.get("colName")}')
    def withColumn(self, colName, col):
        result = self._data.withColumn(colName, col)
        return TidyDataFrame(result, config=self.config, context=self.context)

    @_record(message="calling pipe operator!!!")
    def pipe(self, *funcs: Callable):
        """Chain multiple custom transformation functions to be applied iteratively."""
        result = functools.reduce(
            lambda init, func: init.transform(func), funcs, self._data
        )
        return TidyDataFrame(result, config=self.config, context=self.context)

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
                    self._logger(
                        operation=attr, message="not yet implemented", level="warning"
                    )
                    return TidyDataFrame(
                        result, config=self.config, context=self.context
                    )
                else:
                    return self

            return wrapper
        ### TODO: validate if this logging operation is legit
        ### TODO: mark as unstable (sometimes get notebook dependencies caught in this; generates long message)
        # self._logger(operation=attr, message="method does not exist", level="error")
        raise AttributeError(
            f"'{type(self._data).__name__}' object has no attribute '{attr}'"
        )
