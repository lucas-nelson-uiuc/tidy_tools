import datetime
import pytest

from pyspark.sql import SparkSession, types as T

from tidy_tools.tidy import TidyDataFrame


@pytest.fixture
def spark_fixture():
    spark = SparkSession.builder.appName("Testing PySpark Example").getOrCreate()
    yield spark


@pytest.fixture
def sample_data(spark_fixture):
    data = spark_fixture.createDataFrame(
        [
            {
                "name": "Homer",
                "birth_date": datetime.date(1956, 5, 12),
                "original_air_date": datetime.datetime(1987, 4, 19, 20, 0, 0),
                "seasons": 36,
                "instrument": None,
            },
            {
                "name": "Marge",
                "birth_date": datetime.date(1956, 10, 1),
                "original_air_date": datetime.datetime(1987, 4, 19, 20, 0, 0),
                "seasons": 36,
                "instrument": None,
            },
            {
                "name": "Bart",
                "birth_date": datetime.date(1979, 4, 1),
                "original_air_date": datetime.datetime(1987, 4, 19, 20, 0, 0),
                "seasons": 36,
                "instrument": None,
            },
            {
                "name": "Lisa",
                "birth_date": datetime.date(1981, 5, 9),
                "original_air_date": datetime.datetime(1987, 4, 19, 20, 0, 0),
                "seasons": 36,
                "instrument": "Saxophone",
            },
        ],
        schema=T.StructType(
            [
                T.StructField("name", T.StringType(), nullable=False),
                T.StructField("birth_date", T.DateType(), nullable=False),
                T.StructField("original_air_date", T.TimestampType(), nullable=False),
                T.StructField("seasons", T.IntegerType(), nullable=False),
                T.StructField("instrument", T.StringType(), nullable=True),
            ]
        ),
    )
    yield TidyDataFrame(data)