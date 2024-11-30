from typing import Callable

from attrs import define
from pyspark.sql import DataFrame


@define
class TidyError:
    column: str
    validation: Callable
    data: DataFrame

    def __repr__(self):
        return f"TidyError(column={self.column}, validation={self.validation(self.column)}, data={self.data.count():,} rows)"
