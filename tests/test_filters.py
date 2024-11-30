from pyspark.testing import assertDataFrameEqual
from tidy_tools.core.filters import filter_nulls


class TestFilters:
    def test_filter_nulls(self, eits_data):
        eits_data.filter_nulls = filter_nulls

        # test `filter_nulls` is equivalent to `DataFrame.na.drop`
        assertDataFrameEqual(eits_data.na.drop(how="any"), eits_data.filter_nulls())

        assertDataFrameEqual(
            eits_data.na.drop(how="all"), eits_data.filter_nulls(strict=True)
        )

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
            eits_data.na.drop(subset=[columns]), eits_data.filter_nulls(*columns)
        )

        columns = ["formats", "producer", "ceritifed_gold", "comments"]
        assertDataFrameEqual(
            eits_data.na.drop(subset=[columns]), eits_data.filter_nulls(*columns)
        )

        assert True

    def test_filter_regex(self, eits_data):
        # eits_data.filter_regex = filter_regex
        assert True

    def test_filter_elements(self, eits_data):
        # eits_data.filter_elements = filter_elements
        assert True
