import datetime
import decimal
from types import MappingProxyType

from pyspark.sql import types as T


PYSPARK_TYPES = MappingProxyType(
    {
        str: T.StringType(),
        int: T.IntegerType(),
        float: T.FloatType(),
        decimal.Decimal: T.DecimalType(38, 6),
        datetime.date: T.DateType(),
        datetime.datetime: T.TimestampType(),
    }
)
