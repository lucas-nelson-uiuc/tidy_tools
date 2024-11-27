from typing import Callable, Optional
import functools
import operator

from pyspark.sql import DataFrame, functions as F

from tidy_tools.core._types import ColumnReference


def _join(self, other: DataFrame, on: ColumnReference, how: str, relationship: Optional[str] = "one-to-one") -> DataFrame:


def safely_join(*checks: Callable):
    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(other, *args, **kwargs):


        return wrapper
    
    return decorator



def left_join(self, other: DataFrame, on: ColumnReference) -> DataFrame:
    return self.join(other, on=on, how="left")