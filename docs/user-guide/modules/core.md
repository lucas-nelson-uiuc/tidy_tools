# Core

The `tidy_tools.core` module is a functional replacement of the "basic" queries, namely:

- Selecting columns
- Filtering data

## Motivation

The core module stemmed from a couple ideas, but the most pressing examples are:

- The tedious nature of applying a filter across multiple columns
- The intuitive column selectors module in Polars

For this section, I'll only focus on the first example. Consider the following PySpark
code that attempts to filter null values in a DataFrame.

```python
from pyspark.sql import functions as F

result = spark_data.filter(
    (F.col('A').isNull() | F.col('A').rlike(r"^\s*$")) |
    (F.col('B').isNull() | F.col('B').rlike(r"^\s*$")) |
    (F.col('C').isNull() | F.col('C').rlike(r"^\s*$"))
)
```

Let's read through this query. There are three main aspects:

- our **predicate** checks for values that are `NULL` or entirely whitespace
- our predicate is applied to **multiple columns**
- our predicates are **reduced** into a single expression

The basis of all PySpark queries can be expressed as such: *we want to apply a
predicate to at least one column that evaluates to a PySpark expression.* However,
we are not limited to the PySpark API.

Let's revisit this example using a functional approach:

```python
import functools
import operator

from pyspark.sql import (
    Column,
    DataFrame,
    functions as F
)

def is_null(column: str | Column) -> Column:
    """Return expression identifying null values."""
    if not isinstance(column, Column):
        column = F.col(column)
    return column.isNull() | column.rlike(r"^\s*$")

def filter_null(
    data: DataFrame,
    *columns: str | Column,
    strict: bool = False,
    invert: bool = False
) -> DataFrame:
    """Filter data for null values."""
    # define, apply predicate to multiple columns (by default, all)
    predicate = map(is_null, columns or data.columns)
    # reduce predicates into single expression
    comparison_op = operator.and_ if strict else operator.or_
    query = functools.reduce(comparison_op, predicate)
    # evaluate expression on data
    return data.filter(operator.inv(query) if invert else query)
```

This seems like even more work for filtering null values than before. Luckily only
I will be writing this. Let's see what you'll be writing instead and compare it to
how you would write it in native PySpark.

```python
# filtering one column for null values
pyspark_result = spark_data.filter(F.col('A').isNull() | F.col('A').rlike(r"^\s*$"))
tidy_result = filter_null(spark_data, 'A')

# filtering on multiple columns for *any* null values
pyspark_result = spark_data.filter(
    (F.col('A').isNull() | F.col('A').rlike(r"^\s*$")) |
    (F.col('B').isNull() | F.col('B').rlike(r"^\s*$")) |
    (F.col('C').isNull() | F.col('C').rlike(r"^\s*$"))
)
tidy_result = filter_null(spark_data, 'A', 'B', 'C')

# filtering on multiple columns for *all* null values
pyspark_result = spark_data.filter(
    (F.col('A').isNull() | F.col('A').rlike(r"^\s*$")) &
    (F.col('B').isNull() | F.col('B').rlike(r"^\s*$")) &
    (F.col('C').isNull() | F.col('C').rlike(r"^\s*$"))
)
tidy_result = filter_null(spark_data, 'A', 'B', 'C', strict=True)

# filtering on multiple columns for *no* null values
pyspark_result = spark_data.filter(
    ~ (
        (F.col('A').isNull() | F.col('A').rlike(r"^\s*$")) |
        (F.col('B').isNull() | F.col('B').rlike(r"^\s*$")) |
        (F.col('C').isNull() | F.col('C').rlike(r"^\s*$"))
    )
)
tidy_result = filter_null(spark_data, 'A', 'B', 'C', invert=True)
```

All filtering expressions reduce these tedious elements of PySpark expressions
to intuitive, easy-to-control parameters. This is just one example of how Tidy
Tools promotes declarative workflows, letting you focus on the '*what to do*' and not
the '*how to do*'.
