# Model Module

Class-driven workflows driven by *attrs*.

## Description

The `TidyDataModel` is a custom class that allows you to **express your expectations**
of a data model. In data pipelines, it's essential to know if and when your data no
longer meets your expectations, but how is your pipeline meant to know this?

With a simple class - supported by *attrs* - we can capture all field-level expectations,
including:

- **Type Hints**: what type is your data meant to be?
- **Conversions**: how should values be treated?
- **Validations**: what values should/shouldn't exist?

Let's see how someone would use `TidyDataModel` for their bank statements.

## Usage

Assume you have the following columns in your bank statement:

- `account_no`: the account associated with the transaction
- `posted_date`: the date of the transaction
- `amount`: the amount charged in the currency of the transaction's country
- `description`: details of the transaction (e.g. store, retailer, etc.)

A simple model would include these fields' names and their data types.

```python
import datetime
import decimal

from attrs import define, field
from tidy_tools.model import TidyDataModel


@define
class BankStatement(TidyDataModel):
    """Basic implementation of bank statement data."""
    account_no: str
    posted_date: datetime.date
    amount: decimal.Decimal
    description: str
```

This implementation is similar to creating a `StructType` object to represent
our schema. What's the point? Well, our expectations don't just stop at each
field's name and data type. Let's add more to our model.

```python
from attrs import validators
from pyspark.sql import Column


def convert_currency(currency: str):
    match currency.strip().upper():
        case "CAD":
            currency_rate = 1.44
        case "YEN":
            currency_rate = 70
        case _:
            currency_rate = 1

    def closure(column: Column) -> Column:
        return column * currency_rate
    return closure


@define
class BankStatement(TidyDataModel):
    """Complete implementation of bank statement data."""
    # we expect:
    #   - all account numbers to have a specific format
    account_no: str = field(validator=validators.matches_re(r"\d{5}-\d{3}-\d{2}"))

    # we expect:
    #   - each date to be within the year 2024
    #   - each date to be formatted like this '01/01/1900'
    posted_date: datetime.date = field(
        validators=[
            validators.ge(datetime.date(2024, 1, 1)),
            validators.le(datetime.date(2024, 12, 31))
        ],
        metadata={"format": "MM/dd/yyyy"}
    )

    # we expect:
    #   - currency in 'USD' so we convert to 'CAD'
    amount: decimal.Decimal = field(converter=convert_currency("CAD"))

    # we have no "significant" expectations for `description`
    description: str
```

Models are more than just definitions of data - they are also
actionable objects. Using your model, you can load data directly
into your environment. This step will:

- enforce the model's schema
- concatenate all sources (if multiple passed)
- run all field-level conversions
- run all field-level validations
- return a converted DataFrame

```python
bank_activity = BankStatement.load(
    "activity/january.csv",
    "activity/feburary.csv",
    "activity/march.csv",
    read_func=spark.read.csv,
    read_options={"header": "true"}
)
```
