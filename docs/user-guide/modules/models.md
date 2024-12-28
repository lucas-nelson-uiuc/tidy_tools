# Models

Tidy Tools prioritizes development around the `TidyDataFrame` module. However,
not all workflows require such a hands-on approach, where all conversions
and validations are applied directly on/through the data. Instead, what if you
could reduce these operations to their barest form, abstracting the PySpark
DataFrame API out of your business logic?

The `TidyDataModel` is a class-level implementation of `TidyDataFrame`. Rather
than writing queries one-by-one and having to track things like schema evolution,
(implicit) field validations, and dataframe-level relationships, operations can
be defined all in one place. This greatly simplifies the development and review
process since all things can be tracked back to the `TidyDataModel` instance.

## TidyDataFrame vs TidyDataModel

### Design

As discussed on the previous page, `TidyDataFrame` is a wrapper around the
native PySpark DataFrame class. Although `TidyDataModel` is also a Python
class, it is built on [attrs](https://www.attrs.org/en/stable/), a robust
class building package.

### Usage

All data workflows incorporate the following principals to various degrees:

- **Conversions**: manipulating data into different shapes

- **Validations**: asserting expectations of your data

As we will see below, `TidyDataFrame` - just like the Pyspark DataFrame -
greatly simplifies the conversion workflow by incorporating logging
messages. However, it falls short in the validation aspect, an area that
`TidyDataModel` thrives in. Let's observe both below.

#### Conversions

Let's use the following example based on the California Housing market
dataset. We must perform following conversions:

- Convert `latitude` and `longitude` to float types.

- Scale `income` by the CAD foreign exchange rate.

```python
import decimal

from attrs import define, field

from pyspark.sql import types as T, functions as F

from tidy_tools.dataframe import TidyDataFrame
from tidy_tools.model import TidyDataModel


def convert_fx(currency: str) -> Callable:
    """Closure for returning a function that contains the appropriate currency conversion"""

    match currency.strip().upper():
        case "CAD":
            match_rate = 0.7
        case "YEN":
            match_rate = 70
        case _:
            match_rate = 1

    def convert(column: Column) -> Column:
        """Scale a currency column by the specified rate"""
        return column * match_rate

    return convert


# load data
spark_data = spark.read.csv("california_housing.csv")

# apply conversions with TidyDataFrame
tidy_data = (
    TidyDataFrame(spark_data)
    .withColumns({
        column: F.col(column).cast(T.FloatType())
        for column in (longitude, latitude)
    })
    .withColumn("median_income", convert_fx(currency="CAD")("median_income"))
)

# apply conversions with TidyDataModel
@define
class CaliforniaHousing(TidyDataModel):
    longitude: float
    latitude: float
    median_age: int
    rooms: int
    bedrooms: int
    population: int
    households: int
    median_income: decimal.Decimal = field(converter=convert_fx(currency="CAD"))
    value: decimal.Decimal

tidy_data = CaliforniaHousing.read("california_housing.csv")
```

Using `TidyDataFrame`, we can easily address all requirements using PySpark's
DataFrame API (with the added bonus of line-by-line logging messages). However,
notice that `TidyDataModel` can also perform the task in a syntax that does not
*explicitly* rely on the PySpark DataFrame API. Simply specify what needs to be
converted and nothing more.

Addressing the elephant in the room, `TidyDataModel` will *always* require more
setup than `TidyDataFrame`. This is because a data model should represent a
complete and accurate model of your data, something `TidyDataFrame` cannot
and should not incorporate by default.

#### Validations

Our client is happy with the conversions, but they want to be sure that the
data meets their strict requirements. Let's try to validate the following:

- `latitude` is between (-90, 90) degrees

- `longitude` is between (-180, 180) degrees

```python
# apply conversions, validations with TidyDataFrame
tidy_data = (
    TidyDataFrame(spark_data)
    .withColumns({
        column: F.col(column).cast(T.FloatType())
        for column in (longitude, latitude)
    })
    .withColumn("median_income", convert_fx(currency="CAD")("median_income"))
)
assert tidy_data.filter(~F.col("latitude").between(-90, 90)).isEmpty()
assert tidy_data.filter(~F.col("longitude").between(-180, 180)).isEmpty()

# apply conversions, validations with TidyDataModel
def validate_range(column: Column, lower: int, upper: int) -> Column:
    return column.between(lower, upper)

@define
class CaliforniaHousing(TidyDataModel):
    longitude: float = field(validator=validate_range(-90, 90))
    latitude: float = field(validator=validate_range(-180, 180))
    median_age: int
    rooms: int
    bedrooms: int
    population: int
    households: int
    median_income: decimal.Decimal = field(converter=convert_fx(currency="CAD"))
    value: decimal.Decimal

tidy_data = CaliforniaHousing.read("california_housing.csv")
```

Notice how the `TidyDataFrame` workflow now has two rogue `assert` statements
at the end of the workflow. As of right now, should either condition fail, the
code comes to a halt and the user must debug the error themselves. This task
can be extremely tedious and lost in the midst of all your conversion
operations.

In contrast, `TidyDataModel` encapsulates the validation logic in the same
location as the conversion logic. We already had a clear picture of our
data since the model details all the fields we expect. Now we have an even
clearer picture of what is and is not true of our data for the validations we
specified.

Additionally, notice that this method also required little to no knowledge of
the PySpark DataFrame API or Python's `assert` statement, building on the goal
of separating language-specific features from your business logic.

##### Validators

Similar to the `attrs` package, `tidy-tools` comes with its own set of
validators. These functions evaluate to filtering expressions that attempt to
assert some condition is true. Specifically, given a condition, we expect zero
rows to ***not*** meet said condition.

A list of built-in validators include:

- `validate_nulls`

- `validate_regex`

- `validate_membership`

- `validate_range`

The priority now is to develop native validators that would be helpful for users.
However, the end goal is to only provide validators that cannot be constructed
using the `attrs.validators` module.
