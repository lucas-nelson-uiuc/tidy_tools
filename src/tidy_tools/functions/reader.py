from pathlib import Path
from typing import Callable

from loguru import logger
from pyspark.errors import PySparkException
from pyspark.sql import DataFrame
from tidy_tools.functions.merge import concat


def read(
    *source: str | Path | DataFrame,
    read_func: Callable,
    **read_options: dict,
) -> DataFrame:
    """
    Load data from source(s) as a PySpark DataFrame.

    Parameters
    ----------
    *source : str | Path | DataFrame
        Arbitrary number of data references. If file-like reference, data will
        be loaded using `read_func` and optional `read_options`. If DataFrame,
        data will be returned.
    read_func : Callable
        Function to load data from source(s).
    **read_options : dict
        Additional arguments to pass to the read_function.

    Returns
    -------
    DataFrame
        Object containing data from all source(s) provided.

    Raises
    ------
    PySparkException
        If reading source(s) cannot be performed successfully.
    """

    def _read_func(source: str | Path | DataFrame) -> DataFrame:
        """
        Wrap read function to skip DataFrame instances.

        Parameters
        ----------
        source : str | Path | DataFrame
            Reference to data object.

        Returns
        -------
        DataFrame
            Contents of data object.
        """
        if isinstance(source, DataFrame):
            return source
        return read_func(source, **read_options)

    try:
        logger.info(f"Attempting to load {len(source)} source(s)")
        data = concat(*map(_read_func, source))
        logger.success(f"Loaded {data.count():,} rows.")
    except PySparkException as e:
        logger.error("Reader failed while loading data.")
        raise e
    return data
