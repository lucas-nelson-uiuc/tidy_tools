# Workflow

As of PySpark `v3.5`, the only support for piping methods is invoking
`DataFrame.transform`, like such:

```python
from pyspark.sql import DataFrame


def f(data: DataFrame) -> DataFrame:
    ...

def g(data: DataFrame) -> DataFrame:
    ...

def h(data: DataFrame) -> DataFrame:
    ...


transformed_data = data.transform(f).transform(g).transform(h)
```

Although this approach works, it's limiting since only one custom
transformation function can be applied per `DataFrame.transform`. Additionally,
there is no insight into the procedures, making this difficult to trace should
an error arise. Ideally, users should be able to invoke multiple transformations
in one function call and store procedures to be called at a later time with
insight into the chain of commands.

Tidy Tools addresses this by providing the `workflow` module. Users can expect
to compose simple pipelines that provide visibility into procedures.

```python
from pyspark.sql import DataFrame
from tidy_tools.workflow import pipe, compose


def f(data: DataFrame) -> DataFrame:
    ...

def g(data: DataFrame) -> DataFrame:
    ...

def h(data: DataFrame) -> DataFrame:
    ...


# execute transformation functions in succession
transformed_data = pipe(data, f, g, h)

# or, store transformation functions as single callable function
pipeline = compose(f, g, h)
transformed_data = pipeline(data)
```

Both approaches reduce the manual process of chaining `DataFrame.transform`
multiple times. Additionally, `compose()` lets you store common procedures,
giving users a simple method for chaining transformation functions into a
single function.
