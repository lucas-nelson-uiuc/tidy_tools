import functools
from typing import Callable

from pyspark.sql import Column
from pyspark.sql import DataFrame


def merge(*data: DataFrame, func: Callable, **kwargs: dict) -> DataFrame:
    """
    Merge an arbitrary number of DataFrames into a single DataFrame.

    Parameters
    ----------
    *data : DataFrame
        PySpark DataFrame.
    func : Callable
        Reduce function to merge two DataFrames to each other. By default, this
        union resolves by column name.
    **kwargs : dict, optional
        Keyword-arguments for merge function.

    Returns
    -------
    DataFrame
        Result of merging all `data` objects by `func`.
    """
    func = functools.partial(func, **kwargs)
    return functools.reduce(func, data)


def concat(
    *data: DataFrame,
    func: Callable = DataFrame.unionByName,
    **kwargs: dict,
) -> DataFrame:
    """
    Concatenate an aribitrary number of DataFrames into a single DataFrame.

    By default, all objects are appended to one another by column name. An error
    will be raised if column names do not align.

    Parameters
    ----------
    *data : DataFrame
        PySpark DataFrame.
    func : Callable
        Reduce function to concatenate two DataFrames to each other. By default, this
        union resolves by column name.
    **kwargs : dict, optional
        Keyword-arguments for merge function.

    Returns
    -------
    DataFrame
        Result of concatenating `data`.
    """
    return merge(*data, func=func, **kwargs)


def join(
    *data: DataFrame,
    on: str | Column,
    how: str = "inner",
    func: Callable = DataFrame.join,
    **kwargs: dict,
) -> DataFrame:
    """
    Join an aribitrary number of DataFrames into a single DataFrame.

    By default, all objects are appended to one another by column name. An error
    will be raised if column names do not align.

    Parameters
    ----------
    *data : DataFrame
        PySpark DataFrame.
    on : str | Column
        Column name or expression to perform join.
    how : str
        Set operation to perform.
    func : Callable
        Reduce function to join two DataFrames to each other.
    **kwargs : dict, optional
        Keyword-arguments for merge function.

    Returns
    -------
    DataFrame
        Result of joining `data`.
    """
    return merge(*data, func=func, on=on, how=how, **kwargs)
