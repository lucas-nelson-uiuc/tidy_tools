from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    TidyDataFrame = "TidyDataFrame"

from pyspark.sql import Column, DataFrame


ColumnReference = str | Column
DataFrameReference = DataFrame | Optional[DataFrame]
