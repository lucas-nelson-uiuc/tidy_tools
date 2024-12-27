import datetime

from pyspark.sql import functions as F
from pyspark.testing import assertDataFrameEqual
from tidy_tools.core.filter import filter_elements
from tidy_tools.core.filter import filter_nulls
from tidy_tools.core.filter import filter_range
from tidy_tools.core.filter import filter_regex


class TestFilters:
    def test_filter_nulls(self, eits_data):
        # hypothesis: `strict` parameter behaves like `how` parameter
        assertDataFrameEqual(eits_data.na.drop(how="any"), filter_nulls(eits_data))
        assertDataFrameEqual(
            eits_data.na.drop(how="all"), filter_nulls(eits_data, strict=True)
        )

        # hypothesis: specifying columns behaves same as `subset`
        columns = [
            "title",
            "release_year",
            "release_date",
            "recorded_at",
            "tracks",
            "duration_minutes",
            "rating",
        ]
        assertDataFrameEqual(
            eits_data.na.drop(subset=columns), filter_nulls(eits_data, *columns)
        )
        assertDataFrameEqual(
            eits_data.na.drop(subset=columns, how="all"),
            filter_nulls(eits_data, *columns, strict=True),
        )

    def test_filter_regex(self, eits_data):
        # hypothesis: `filter_regex` constructs valid substring filtering queries
        TEST_PATTERN: str = r","
        assertDataFrameEqual(
            eits_data.filter(F.col("title").rlike(TEST_PATTERN)),
            filter_regex(eits_data, "title", pattern=TEST_PATTERN),
        )
        assertDataFrameEqual(
            eits_data.filter(
                F.col("title").rlike(TEST_PATTERN)
                | F.col("comments").rlike(TEST_PATTERN)
            ),
            filter_regex(eits_data, "title", "comments", pattern=TEST_PATTERN),
        )
        # hypothesis: `filter_regex` can handle logical operations
        assertDataFrameEqual(
            eits_data.filter(
                F.col("title").rlike(TEST_PATTERN)
                & F.col("comments").rlike(TEST_PATTERN)
            ),
            filter_regex(
                eits_data, "title", "comments", pattern=TEST_PATTERN, strict=True
            ),
        )
        assertDataFrameEqual(
            eits_data.filter(
                ~(
                    F.col("title").rlike(TEST_PATTERN)
                    | F.col("comments").rlike(TEST_PATTERN)
                )
            ),
            filter_regex(
                eits_data, "title", "comments", pattern=TEST_PATTERN, invert=True
            ),
        )
        assertDataFrameEqual(
            eits_data.filter(
                ~(
                    F.col("title").rlike(TEST_PATTERN)
                    & F.col("comments").rlike(TEST_PATTERN)
                )
            ),
            filter_regex(
                eits_data,
                "title",
                "comments",
                pattern=TEST_PATTERN,
                strict=True,
                invert=True,
            ),
        )

    def test_filter_elements(self, eits_data):
        TEST_ELEMENTS: list[str] = [
            ["CD", "Vinyl"],
            ["CD", "Digital"],
            "john congleton",
        ]
        assertDataFrameEqual(
            eits_data.filter(F.col("formats").isin(TEST_ELEMENTS)),
            filter_elements(eits_data, "formats", elements=TEST_ELEMENTS),
        )
        assertDataFrameEqual(
            eits_data.filter(
                F.col("formats").isin(TEST_ELEMENTS)
                | F.col("producer").isin(TEST_ELEMENTS)
            ),
            filter_elements(eits_data, "formats", "producer", elements=TEST_ELEMENTS),
        )
        assertDataFrameEqual(
            eits_data.filter(
                F.col("formats").isin(TEST_ELEMENTS)
                & F.col("producer").isin(TEST_ELEMENTS)
            ),
            filter_elements(
                eits_data, "formats", "producer", elements=TEST_ELEMENTS, strict=True
            ),
        )
        assertDataFrameEqual(
            eits_data.filter(
                ~(
                    F.col("formats").isin(TEST_ELEMENTS)
                    | F.col("producer").isin(TEST_ELEMENTS)
                )
            ),
            filter_elements(
                eits_data, "formats", "producer", elements=TEST_ELEMENTS, invert=True
            ),
        )
        assertDataFrameEqual(
            eits_data.filter(
                ~(
                    F.col("formats").isin(TEST_ELEMENTS)
                    & F.col("producer").isin(TEST_ELEMENTS)
                )
            ),
            filter_elements(
                eits_data,
                "formats",
                "producer",
                elements=TEST_ELEMENTS,
                strict=True,
                invert=True,
            ),
        )

    def test_filter_range(self, eits_data):
        TEST_LOWER_BOUND: datetime.date = datetime.date(2001, 1, 1)
        TEST_UPPER_BOUND: datetime.date = datetime.date(2015, 12, 31)

        assertDataFrameEqual(
            eits_data.filter(
                F.col("release_date").between(TEST_LOWER_BOUND, TEST_UPPER_BOUND)
            ),
            filter_range(
                eits_data,
                "release_date",
                lower_bound=TEST_LOWER_BOUND,
                upper_bound=TEST_UPPER_BOUND,
            ),
        )

        assertDataFrameEqual(
            eits_data.filter(
                F.col("release_date").between(TEST_LOWER_BOUND, TEST_UPPER_BOUND)
                | F.col("recorded_at").between(TEST_LOWER_BOUND, TEST_UPPER_BOUND)
            ),
            filter_range(
                eits_data,
                "release_date",
                "recorded_at",
                lower_bound=TEST_LOWER_BOUND,
                upper_bound=TEST_UPPER_BOUND,
            ),
        )

        assertDataFrameEqual(
            eits_data.filter(
                F.col("release_date").between(TEST_LOWER_BOUND, TEST_UPPER_BOUND)
                & F.col("recorded_at").between(TEST_LOWER_BOUND, TEST_UPPER_BOUND)
            ),
            filter_range(
                eits_data,
                "release_date",
                "recorded_at",
                lower_bound=TEST_LOWER_BOUND,
                upper_bound=TEST_UPPER_BOUND,
                strict=True,
            ),
        )

        assertDataFrameEqual(
            eits_data.filter(
                ~(
                    F.col("release_date").between(TEST_LOWER_BOUND, TEST_UPPER_BOUND)
                    | F.col("recorded_at").between(TEST_LOWER_BOUND, TEST_UPPER_BOUND)
                )
            ),
            filter_range(
                eits_data,
                "release_date",
                "recorded_at",
                lower_bound=TEST_LOWER_BOUND,
                upper_bound=TEST_UPPER_BOUND,
                invert=True,
            ),
        )

        assertDataFrameEqual(
            eits_data.filter(
                ~(
                    F.col("release_date").between(TEST_LOWER_BOUND, TEST_UPPER_BOUND)
                    & F.col("recorded_at").between(TEST_LOWER_BOUND, TEST_UPPER_BOUND)
                )
            ),
            filter_range(
                eits_data,
                "release_date",
                "recorded_at",
                lower_bound=TEST_LOWER_BOUND,
                upper_bound=TEST_UPPER_BOUND,
                strict=True,
                invert=True,
            ),
        )
