from typing import Callable

from attrs import define
from pyspark.sql import DataFrame


@define
class TidyError:
    """
    Class for handling errors in TidyDataFrame.

    Parameters
    ----------
    column : str
        Column to perform check on.
    validation : Callable
        Validation to call on data with column.
    data : DataFrame
        Failures from resulting validation.
    """

    column: str
    validation: Callable
    data: DataFrame

    def __repr__(self):
        return f"TidyError(column={self.column}, validation={self.validation(self.column)}, data={self.data.count():,} rows)"
