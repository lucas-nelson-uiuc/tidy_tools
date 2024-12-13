import random

import pyspark.pandas as ps
from pyspark.sql import DataFrame


def generate_sample(n_row: int, schema: dict) -> DataFrame:
    """
    Generate synthetic data according to the provided schema.

    Parameters
    ----------
    n_row : int
        Number of rows to randomly generate.
    schema : dict
        Key-value pairs where each item contains a column (str) and set of values to sample from.

    Returns
    -------
    DataFrame
        Randomly generated data of `n_row` given `schema`.

    Examples
    --------
    >>> schema = {
        "name": ["Homer", "Marge", "Bart", "Lisa"],
        "hometown": ["Springfield", "Not Springfield"]
    }
    >>> generate_data(n_row=3, schema=schema)
    DataFrame[name: string, hometown: string]
    """
    sample = ps.DataFrame(
        {
            "id": range(n_row),
            **{
                column: random.choices(values, k=n_row)
                for column, values in schema.items()
            },
        }
    )
    return sample.to_spark()
