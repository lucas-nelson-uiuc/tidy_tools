import functools
from pathlib import Path
from typing import Callable

from loguru import logger
from pyspark.errors import PySparkException
from pyspark.sql import DataFrame


def read(
    *source: str | Path,
    read_func: Callable,
    merge_func: bool | Callable = DataFrame.unionByName,
    **read_options: dict,
) -> dict[DataFrame] | DataFrame:
    """
    Load data from source(s) as a PySpark DataFrame.

    Parameters
    ----------
    *source : str | Path
        Arbitrary number of file references.
    read_func : Callable
        Function to load data from source(s).
    merge_func : Optional[Callable]
        Function to merge data from sources. Only applied if multiple sources are provided.
    **read_options : dict
        Additional arguments to pass to the read_function.
    """
    read_func = functools.partial(read_func, **read_options)
    try:
        logger.info(f"Attempting to read {len(source)} source(s): {', '.join(source)}")
        data = functools.reduce(merge_func, map(read_func, source))
        logger.success(f"Loaded {data.count():,} rows.")
    except PySparkException as e:
        logger.error("Reader failed while loading data.")
        raise e
