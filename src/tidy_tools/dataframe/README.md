# DataFrame Module

Configurable extension of PySpark DataFrame API.

## Description

The `TidyDataFrame` is a custom class that primarily allows you to configure
how your DataFrame executes during its lifetime. The most beneficial of these
configurations is the built-in logging process (inspired by tidylog in R). However,
additional configurations include:

- Sisabling count/display operations
- Specifying multiple logging handlers
- Documenting workflows in code

On top of these configurations, `TidyDataFrame` also offers experimental features
as seen throughout the rest of the Tidy Tools package, including:

- Column selectors (like Polars)
- Functional filters
- Pythonic API

Let's see how someone would use `TidyDataFrame`.

## Usage

A simple example shows the immediate benefits of `TidyDataFrame`:

```python
from tidy_tools.dataframe import TidyDataFrame


# with PySpark, I do not know the impact of each query
result = (
    spark_data
    .select(...)        # how many columns did I select?
    .filter(...)        # how many rows did I remove?
    .withColumn(...)    # what column did I add to my DataFrame?
)

# with TidyDataFrame, I know the exact impact of each query
result = (
    TidyDataFrame(spark_data)
    .select(...)
    .filter(...)
    .withColumn(...)
    ._data
)
#> INFO    | enter: TidyDataFrame[M rows x N cols]
#> INFO    | select: selected Y columns
#> INFO    | filter: removed X rows, M-X remaining
#> INFO    | mutate: added `<column-name>` (type: <column-type>)
#> INFO    | exit: DataFrame[M-X rows x N-Y+1 cols]
```

With minor modifications to your existing code, you receive this immensely
beneficial insight into your workflow as it is happening in real-time. This
eliminates any need to verify the correctness of your code after running it
since you understand exactly what is happening.

Let's see how else we can use TidyDataFrame.

```python
from tidy_tools.dataframe import TidyContext
from tidy_tools.dataframe import TidyLogHandler, TidyFileHandler


# 01. simple configuration
context = TidyContext(name="Cool Data", display=False)
TidyDataFrame(spark_data, context).display()
#> INFO    | Cool Data[M rows x N cols]
#> WARNING | display: disabled by context


# 02. passing multiple logging handlers (logs to all sinks provided)
context = TidyContext(name="Logging Data", handlers=[TidyLogHandler(), TidyFileHandler("logging_data.log")])
(
    TidyDataFrame(spark_data, context)
    .comment("Removing invalid entries from dataset") # persist comments to logs
    .filter(...)
)
#> INFO    | Logging Data [M rows x N cols]
#> INFO    | comment: Removing entries from dataset
#> INFO    | filter: removed X rows, M-X remaining
```
