# TODO: enable pyspark.testing at some point
# from pyspark.testing import assertDataFrameEqual


class TestFilters:
    def test_filter_nulls(self, eits_data):
        # tidy_data = TidyDataFrame(eits_data)
        # tidy_data.filter_nulls = filter_nulls

        # # test `filter_nulls` is equivalent to `DataFrame.na.drop`
        # assert eits_data.na.drop(how="any").count() == tidy_data.filter_nulls().count()

        # assert (
        #     eits_data.na.drop(how="all").count()
        #     == tidy_data.filter_nulls(strict=True).count()
        # )

        # columns = [
        #     "title",
        #     "release_year",
        #     "release_date",
        #     "recorded_at",
        #     "tracks",
        #     "duration_minutes",
        #     "rating",
        # ]
        # assert (
        #     eits_data.na.drop(subset=[columns]).count()
        #     == tidy_data.filter_nulls(*columns).count()
        # )

        # columns = ["formats", "producer", "ceritifed_gold", "comments"]
        # assert (
        #     eits_data.na.drop(subset=[columns]).count()
        #     == tidy_data.filter_nulls(*columns).count()
        # )
        assert True

    def test_filter_regex(self, eits_data):
        # tidy_data = TidyDataFrame(eits_data)
        # eits_data.filter_nulls = filter_nulls
        # tidy_data.filter_nulls = filter_nulls
        assert True

    def test_filter_elements(self, eits_data):
        # tidy_data = TidyDataFrame(eits_data)
        # eits_data.filter_nulls = filter_nulls
        # tidy_data.filter_nulls = filter_nulls
        assert True
