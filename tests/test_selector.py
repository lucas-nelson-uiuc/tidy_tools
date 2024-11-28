import pytest
import datetime

from pyspark.sql import SparkSession, types as T

from tidy_tools.core import selector as cs
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


class TestColumnSelector:
    def test_string(self, sample_data):
        selected_schema = sample_data.select(cs.string())
        assert selected_schema.columns == ["name", "instrument"]

    def test_numeric(self, sample_data):
        selected_schema = sample_data.select(cs.numeric())
        assert selected_schema.columns == ["seasons"]

    def test_temporal(self, sample_data):
        selected_schema = sample_data.select(cs.temporal())
        assert selected_schema.columns == ["birth_date", "original_air_date"]

    def test_date(self, sample_data):
        selected_schema = sample_data.select(cs.date())
        assert selected_schema.columns == ["birth_date"]

    def test_interval(self, sample_data):
        selected_schema = sample_data.select(cs.interval())
        assert selected_schema.columns == []

    def test_complex(self, sample_data):
        selected_schema = sample_data.select(cs.complex())
        assert selected_schema.columns == []

    def test_required(self, sample_data):
        selected_schema = sample_data.select(cs.required())
        assert selected_schema.columns == [
            "name",
            "birth_date",
            "original_air_date",
            "seasons",
        ]

    def test_matches(self, sample_data):
        selected_schema = sample_data.select(cs.matches("_"))
        assert selected_schema.columns == ["birth_date", "original_air_date"]
        selected_schema = sample_data.select(cs.matches("date$"))
        assert selected_schema.columns == ["birth_date", "original_air_date"]
        selected_schema = sample_data.select(cs.matches("*"))
        assert selected_schema.columns == [
            "name",
            "birth_date",
            "original_air_date",
            "seasons",
            "instrument",
        ]

    def test_contains(self, sample_data):
        selected_schema = sample_data.select(cs.contains("_"))
        assert selected_schema.columns == ["birth_date", "original_air_date"]
        selected_schema = sample_data.select(cs.contains("me"))
        assert selected_schema.columns == ["name", "instrument"]
        selected_schema = sample_data.select(cs.contains("krusty"))
        assert selected_schema.columns == []

    def test_starts_with(self, sample_data):
        selected_schema = sample_data.select(cs.starts_with("o"))
        assert selected_schema.columns == ["original_air_date"]
        selected_schema = sample_data.select(cs.starts_with("z"))
        assert selected_schema.columns == []

    def test_ends_with(self, sample_data):
        selected_schema = sample_data.select(cs.ends_with("e"))
        assert selected_schema.columns == ["name", "birth_date", "original_air_date"]
        selected_schema = sample_data.select(cs.ends_with("date"))
        assert selected_schema.columns == ["birth_date", "original_air_date"]
        selected_schema = sample_data.select(cs.ends_with("z"))
        assert selected_schema.columns == []
