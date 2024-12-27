from tidy_tools.core import selector as cs


class TestColumnSelector:
    def test_string(self, simpsons_data):
        selected_columns = simpsons_data.select(cs.string()).columns
        expected_columns = ["name", "instrument"]
        assert set(selected_columns).difference(expected_columns) == set()

    def test_numeric(self, simpsons_data):
        selected_columns = simpsons_data.select(cs.numeric()).columns
        expected_columns = ["seasons"]
        assert set(selected_columns).difference(expected_columns) == set()

    def test_temporal(self, simpsons_data):
        selected_columns = simpsons_data.select(cs.temporal()).columns
        expected_columns = ["birth_date", "original_air_date"]
        assert set(selected_columns).difference(expected_columns) == set()

    def test_date(self, simpsons_data):
        selected_columns = simpsons_data.select(cs.date()).columns
        expected_columns = ["birth_date"]
        assert set(selected_columns).difference(expected_columns) == set()

    def test_time(self, simpsons_data):
        selected_columns = simpsons_data.select(cs.time()).columns
        expected_columns = ["original_air_date"]
        assert set(selected_columns).difference(expected_columns) == set()

    def test_interval(self, simpsons_data):
        selected_columns = simpsons_data.select(cs.interval()).columns
        expected_columns = []
        assert set(selected_columns).difference(expected_columns) == set()

    def test_complex(self, simpsons_data):
        selected_columns = simpsons_data.select(cs.complex()).columns
        expected_columns = []
        assert set(selected_columns).difference(expected_columns) == set()

    def test_required(self, simpsons_data):
        selected_columns = simpsons_data.select(cs.required()).columns
        expected_columns = ["name", "birth_date", "original_air_date", "seasons"]
        assert set(selected_columns).difference(expected_columns) == set()

    def test_matches(self, simpsons_data):
        selected_columns = simpsons_data.select(cs.matches("_")).columns
        expected_columns = [
            "birth_date",
            "original_air_date",
        ]
        assert set(selected_columns).difference(expected_columns) == set()

        selected_columns = simpsons_data.select(cs.matches("date$")).columns
        expected_columns = [
            "birth_date",
            "original_air_date",
        ]
        assert set(selected_columns).difference(expected_columns) == set()

        selected_columns = simpsons_data.select(cs.matches(".*")).columns
        expected_columns = [
            "name",
            "birth_date",
            "original_air_date",
            "seasons",
            "instrument",
        ]
        assert set(selected_columns).difference(expected_columns) == set()

    def test_contains(self, simpsons_data):
        selected_columns = simpsons_data.select(cs.contains("_")).columns
        expected_columns = [
            "birth_date",
            "original_air_date",
        ]
        assert set(selected_columns).difference(expected_columns) == set()

        selected_columns = simpsons_data.select(cs.contains("me")).columns
        expected_columns = ["name", "instrument"]
        assert set(selected_columns).difference(expected_columns) == set()

        selected_columns = simpsons_data.select(cs.contains("krusty")).columns
        expected_columns = []
        assert set(selected_columns).difference(expected_columns) == set()

    def test_starts_with(self, simpsons_data):
        selected_columns = simpsons_data.select(cs.starts_with("o")).columns
        expected_columns = ["original_air_date"]
        assert set(selected_columns).difference(expected_columns) == set()

        selected_columns = simpsons_data.select(cs.starts_with("z")).columns
        expected_columns = []
        assert set(selected_columns).difference(expected_columns) == set()

    def test_ends_with(self, simpsons_data):
        selected_columns = simpsons_data.select(cs.ends_with("e")).columns
        expected_columns = [
            "name",
            "birth_date",
            "original_air_date",
        ]
        assert set(selected_columns).difference(expected_columns) == set()

        selected_columns = simpsons_data.select(cs.ends_with("date")).columns
        expected_columns = [
            "birth_date",
            "original_air_date",
        ]
        assert set(selected_columns).difference(expected_columns) == set()

        selected_columns = simpsons_data.select(cs.ends_with("z")).columns
        expected_columns = []
        assert set(selected_columns).difference(expected_columns) == set()
