import datetime
import tempfile

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from tidy_tools.dataframe import TidyDataFrame


@pytest.fixture
def spark_fixture():  # numpydoc ignore=PR01,YD01
    spark = SparkSession.builder.appName("Testing PySpark Example").getOrCreate()
    yield spark


@pytest.fixture
def log_file():  # numpydoc ignore=PR01,YD01
    yield tempfile.TemporaryFile(mode="w+")


@pytest.fixture
def simpsons_data(spark_fixture):  # numpydoc ignore=PR01,YD01
    """Sample dataset for The Simpsons characters."""
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


@pytest.fixture
def eits_data(spark_fixture):  # numpydoc ignore=PR01,YD01
    """Sample dataset for Explosions in the Sky albums."""
    data = spark_fixture.createDataFrame(
        [
            {
                "title": "how strange, innocence",
                "release_year": 2000,
                "release_date": datetime.date(2000, 8, 29),
                "recorded_at": datetime.datetime(2000, 2, 15, 14, 30),
                "tracks": 8,
                "duration_minutes": 50.3,
                "rating": 4.5,
                "formats": ["CD", "Vinyl", "Digital"],
                "producer": None,
                "certified_gold": False,
                "comments": None,
            },
            {
                "title": "those who tell the truth shall die, those who tell the truth shall live forever",
                "release_year": 2001,
                "release_date": datetime.date(2001, 9, 4),
                "recorded_at": datetime.datetime(2001, 5, 10, 9, 0),
                "tracks": 6,
                "duration_minutes": 50.0,
                "rating": 4.6,
                "formats": ["Vinyl", "Digital"],
                "producer": "john congleton",
                "certified_gold": True,
                "comments": "Rumored to predict 9/11 due to cover art controversy.",
            },
            {
                "title": "the earth is not a cold dead place",
                "release_year": 2003,
                "release_date": datetime.date(2003, 11, 4),
                "recorded_at": datetime.datetime(2003, 7, 14, 13, 45),
                "tracks": 5,
                "duration_minutes": 45.0,
                "rating": 4.9,
                "formats": ["CD", "Digital"],
                "producer": "explosions in the sky",
                "certified_gold": False,
                "comments": "Considered their magnum opus by many fans.",
            },
            {
                "title": "all of a sudden i miss everyone",
                "release_year": 2007,
                "release_date": datetime.date(2007, 2, 20),
                "recorded_at": datetime.datetime(2006, 8, 18, 17, 45),
                "tracks": 6,
                "duration_minutes": 43.8,
                "rating": 4.7,
                "formats": ["CD", "Vinyl"],
                "producer": "john congleton",
                "certified_gold": False,
                "comments": None,
            },
            {
                "title": "take care, take care, take care",
                "release_year": 2011,
                "release_date": datetime.date(2011, 4, 26),
                "recorded_at": datetime.datetime(2010, 11, 1, 12, 0),
                "tracks": 6,
                "duration_minutes": 46.2,
                "rating": 4.3,
                "formats": ["Vinyl", "Digital"],
                "producer": None,
                "certified_gold": True,
                "comments": "Packaged in an elaborate folding album cover.",
            },
            {
                "title": "the wilderness",
                "release_year": 2016,
                "release_date": datetime.date(2016, 4, 1),
                "recorded_at": datetime.datetime(2015, 9, 22, 15, 30),
                "tracks": 9,
                "duration_minutes": 49.2,
                "rating": 4.4,
                "formats": ["Digital"],
                "producer": "john congleton",
                "certified_gold": None,
                "comments": None,
            },
        ],
        schema=T.StructType(
            [
                T.StructField("title", T.StringType(), nullable=False),
                T.StructField("release_year", T.IntegerType(), nullable=False),
                T.StructField("release_date", T.DateType(), nullable=False),
                T.StructField("recorded_at", T.TimestampNTZType(), nullable=False),
                T.StructField("tracks", T.IntegerType(), nullable=False),
                T.StructField("duration_minutes", T.FloatType(), nullable=False),
                T.StructField("rating", T.FloatType(), nullable=False),
                T.StructField("formats", T.ArrayType(T.StringType()), nullable=True),
                T.StructField("producer", T.StringType(), nullable=True),
                T.StructField("certified_gold", T.BooleanType(), nullable=True),
                T.StructField("comments", T.StringType(), nullable=True),
            ]
        ),
    )
    # TODO: yield data, TidyDataFrame(data)
    yield data
