import datetime
import difflib
from contextlib import contextmanager

from attrs import define
from attrs import field
from loguru import logger
from pyspark.sql import types as T


logger.level("ENTER", no=98, color="<green>")
logger.level("EXIT", no=99, color="<red>")


@define
class TidySnapshot:
    operation: str
    message: str
    schema: T.StructType
    dimensions: tuple[int, int]
    timestamp: datetime.datetime = field(default=datetime.datetime.now())


@contextmanager
def tidycontext():
    """Define context manager for handling tidy operations."""
    context = {"operation_log": [], "snapshots": []}
    try:
        logger.log("ENTER", ">> Converting data to TidyDataFrame")
        yield context
        logger.log("EXIT", "<< Returning data as DataFrame")
    finally:
        for log in context["operation_log"]:
            print(log)


def compute_delta(snapshot1: TidySnapshot, snapshot2: TidySnapshot):
    # Get schema differences using difflib
    schema_diff = compare_schemas(snapshot1.schema, snapshot2.schema)
    print("Schema Changes:")
    print("\n".join(schema_diff))

    # Get dimension (row/column count) differences using difflib
    dimension_diff = compare_dimensions(snapshot1.dimensions, snapshot2.dimensions)
    print("Dimension Changes:")
    print("\n".join(dimension_diff))


def compare_schemas(schema1, schema2):
    # Extract column names and types for comparison
    cols1 = [f"{field.name}: {field.dataType}" for field in schema1.fields]
    cols2 = [f"{field.name}: {field.dataType}" for field in schema2.fields]

    # Use difflib to compare column lists
    return list(difflib.ndiff(cols1, cols2))


def compare_dimensions(dim1, dim2):
    # Compare row and column counts as text for difflib
    row_diff = f"Rows: {dim1[0]} -> {dim2[0]}"
    col_diff = f"Columns: {dim1[1]} -> {dim2[1]}"

    # Using difflib to show dimension changes
    return list(difflib.ndiff([row_diff], [col_diff]))
