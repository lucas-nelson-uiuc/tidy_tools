import functools
from typing import Callable
from typing import Optional

from pyspark.sql import DataFrame


def concat(
    *data: DataFrame, func: Optional[Callable] = DataFrame.unionByName, **kwargs: dict
) -> DataFrame:
    func = functools.partial(func, **kwargs)
    return functools.reduce(func, data)
