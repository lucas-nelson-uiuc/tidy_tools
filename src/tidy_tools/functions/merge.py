import functools
from typing import Callable

from pyspark.sql import DataFrame


def concat(
    *data: DataFrame, merge_func: Callable = DataFrame.unionByName, **merge_kwargs: dict
) -> DataFrame:
    """
    Concatenate an arbitrary number of DataFrames into a single DataFrame.

    By default, all objects are appended to one another by column name. An error
    will be raised if column names do not align.

    Parameters
    ----------
    *data : DataFrame
        PySpark DataFrame.
    merge_func : Callable, optional
        Reduce function to merge two DataFrames to each other. By default, this
        union resolves by column name.
    **merge_kwargs : dict, optional
        Keyword-arguments for merge function.

    Returns
    -------
    DataFrame
        Result of merging all `data` objects by `merge_func`.
    """
    # TODO: include logging mechanism
    # TODO: include error handling mechanism
    merge_func = functools.partial(merge_func, **merge_kwargs)
    return functools.reduce(merge_func, data)
