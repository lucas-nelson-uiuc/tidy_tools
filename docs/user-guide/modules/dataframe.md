# DataFrame

The `tidy_tools.dataframe` module offers the most direct extension of the PySpark
DataFrame by offering a wrapper called `TidyDataFrame` - the initial motivation for
Tidy Tools.

This module attempts to achieve the following goals:

- Promoting user-friendly features:
    - Built-in logging (inspired by `tidylog` in R)
    - Contextual evaluation
- Incorporting the rest of the Tidy Tools ecosystem

## TidyDataFrame

Starting out as a basic Python class, TidyDataFrame has evolved from a dataframe
with built-in logging to providing functional recipes to handling user-specific
configurations and more. All methods provided in Tidy Tools are supported by
`TidyDataFrame` as it is designed with this class in mind.

Let's look at an example of how to use `TidyDataFrame` compared to a PySpark
DataFrame.

## Entering and Exiting TidyDataFrame

Creating a `TidyDataFrame` is as simple as wrapping your existing PySpark
DataFrame with the `TidyDataFrame()` function. Once inside a TidyDataFrame,
all supported methods will output a one-line message to the console detailing
the exact impact it has on your data.

```python
from tidy_tools.dataframe import TidyDataFrame


spark_data = ...
tidy_data = TidyDataFrame(spark_data) # initialize a TidyDataFrame instance
```

Once you no longer require the logging services of a `TidyDataFrame`, you may
exit the context by calling `TidyDataFrame.data`. This method will return the
underlying PySpark DataFrame stored inside the tidy context.

```python
underlying_data = tidy_data.data
```

Altogether, a *tidy workflow* will look like this:

```python
# create PySpark DataFrame
spark_data = ...

# convert to a TidyDataFrame
tidy_data = TidyDataFrame(spark_data)

# perform native operations - messages returned to console
tidy_data = (
    tidy_data
    .select(...)
    .filter(...)
    .withColumn(...)
)

# convert back to a PySpark DataFrame
spark_data = tidy_data.data
```

## Extending the TidyDataFrame

Discuss `TidyContext`...
