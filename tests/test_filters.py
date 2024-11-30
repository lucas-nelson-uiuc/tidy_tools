# TODO: enable pyspark.testing at some point
# from pyspark.testing import assertDataFrameEqual
from tidy_tools.core.filters import filter_nulls


class TestFilters:
    def test_filter_nulls(self, eits_data):
        eits_data.filter_nulls = filter_nulls

        # test `filter_nulls` is equivalent to `DataFrame.na.drop`
        assert eits_data.na.drop(how="any").count() == eits_data.filter_nulls().count()

        assert (
            eits_data.na.drop(how="all").count()
            == eits_data.filter_nulls(strict=True).count()
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
        assert (
            eits_data.na.drop(subset=[columns]).count()
            == eits_data.filter_nulls(*columns).count()
        )

        columns = ["formats", "producer", "ceritifed_gold", "comments"]
        assert (
            eits_data.na.drop(subset=[columns]).count()
            == eits_data.filter_nulls(*columns).count()
        )

    def test_filter_regex(self, eits_data):
        # eits_data.filter_regex = filter_regex
        assert True

    def test_filter_elements(self, eits_data):
        # eits_data.filter_elements = filter_elements
        assert True
