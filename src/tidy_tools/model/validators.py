from pyspark.sql import DataFrame
from tidy_tools.core.filter import filter_nulls


def validate_nulls(data: DataFrame, *columns: str, **kwargs) -> bool:
    """
    Validate column(s) do not contain null values.

    Parameters
    ----------
    data : DataFrame
        PySpark DataFrame.
    *columns : str
        Column(s) to validate from `data`.
    **kwargs : dict
        Additional arguments for validation function. See `filter_nulls`.

    Returns
    -------
    bool
        `True` if there are no nulls in column(s); else, False.

    Raises
    ------
    ValueError
        If column(s) provided do not exist in `data`.
    AssertionError
        If null values are detected, an assertion error is raised.
    """
    missing_columns = set(columns).difference(data.columns)
    if missing_columns:
        raise ValueError(f"Columns do not exist: {', '.join(missing_columns)}")
    nulls = data.filter(filter_nulls(*columns, **kwargs))
    return nulls.isEmpty()
