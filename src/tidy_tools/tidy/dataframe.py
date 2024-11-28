from typing import Callable
import inspect
import warnings

import functools

from attrs import define, field
from loguru import logger

from pyspark.sql import DataFrame

from tidy_tools.core.selector import ColumnSelector


@define
class TidyDataFrame:
    _data: str
    config: dict = field(factory=dict)

    def __attrs_post_init__(self):
        self.config.setdefault("register_tidy", True)
        if self.config.get("register_tidy"):
            # TODO: add feature to selectively register modules
            # self.__class__.register(tidy_filter)
            # self.__class__.register(tidy_select)
            # self.__class__.register(tidy_summarize)
            pass

    @classmethod
    def register(cls, module):
        """Register external functions as methods of TidyDataFrame."""
        for name, func in inspect.getmembers(module, inspect.isfunction):
            setattr(cls, name, func)

    def _logger(self, operation: Callable, message: str, level: str = "info") -> None:
        getattr(logger, level)(f"#> {operation:<12}: {message}")
        return self

    @property
    def schema(self):
        return self._data.schema

    @property
    def columns(self):
        return self._data.columns

    def select(self, *selectors: ColumnSelector, strict: bool = True):
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
        if len(selected) < 1:
            warnings.warn("No columns matched the selector(s).")
        self._data = self._data.select(*selected)
        return self

    def pipe(self, *funcs: Callable):
        self._data = functools.reduce(
            lambda init, func: init.transform(func), funcs, self._data
        )
        return self

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
                    self._data = result
                    self._logger(
                        operation=attr, message="not yet implemented", level="warning"
                    )
                    return self
                else:
                    return self

            return wrapper
        ### TODO: validate if this logging operation is legit
        ### TODO: mark as unstable (sometimes get notebook dependencies caught in this; generates long message)
        # self._logger(operation=attr, message="method does not exist", level="error")
        raise AttributeError(
            f"'{type(self._data).__name__}' object has no attribute '{attr}'"
        )
