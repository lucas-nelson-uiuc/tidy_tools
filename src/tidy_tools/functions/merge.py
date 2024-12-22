import functools
from typing import Callable

from pyspark.sql import DataFrame


def merge(*data: DataFrame, func: Callable, **kwargs: dict) -> DataFrame:
    """
    Concatenate an arbitrary number of DataFrames into a single DataFrame.

    By default, all objects are appended to one another by column name. An error
    will be raised if column names do not align.

    Parameters
    ----------
    *data : DataFrame
        PySpark DataFrame.
    func : Callable, optional
        Reduce function to merge two DataFrames to each other. By default, this
        union resolves by column name.
    **kwargs : dict, optional
        Keyword-arguments for merge function.

    Returns
    -------
    DataFrame
        Result of merging all `data` objects by `merge_func`.
    """
    func = functools.partial(func, **kwargs)
    return functools.reduce(func, data)


def concat(
    *data: DataFrame,
    func: Callable = DataFrame.unionByName,
    **kwargs: dict,
):
    return merge(*data, merge_func=func, **kwargs)


def join(
    *data: DataFrame, func: Callable = DataFrame.join, **kwargs: dict
) -> DataFrame:
    return merge(*data, func=func, **kwargs)
