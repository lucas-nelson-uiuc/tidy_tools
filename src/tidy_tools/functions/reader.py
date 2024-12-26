import functools
from pathlib import Path
from typing import Callable

from loguru import logger
from pyspark.errors import PySparkException
from pyspark.sql import DataFrame
from tidy_tools.functions.merge import concat


def read(
    *source: str | Path,
    read_func: Callable,
    **read_options: dict,
) -> DataFrame:
    """
    Load data from source(s) as a PySpark DataFrame.

    Parameters
    ----------
    *source : str | Path
        Arbitrary number of file references.
    read_func : Callable
        Function to load data from source(s).
    **read_options : dict
        Additional arguments to pass to the read_function.

    Returns
    -------
    DataFrame
        Object containing data from all source(s) provided.
    """

    read_func = functools.partial(read_func, **read_options)
    try:
        logger.info(f"Attempting to load {len(source)} source(s)")
        data = concat(*map(read_func, source))
        logger.success(f"Loaded {data.count():,} rows.")
    except PySparkException as e:
        logger.error("Reader failed while loading data.")
        raise e
    return data
