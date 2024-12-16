import pytest
from tidy_tools.workflow.pipeline import compose
from tidy_tools.workflow.pipeline import pipe


class TestPipeline:
    @pytest.mark.parametrize(
        "functions,initial,result",
        [
            ((lambda x: x,), 12, 12),
            (
                (
                    lambda x: x + 10,
                    lambda x: x - 10,
                ),
                12,
                12,
            ),
            ((lambda x: bool(x),), 12, True),
        ],
    )
    def test_pipe(self, functions, initial, result):
        assert pipe(initial, *functions) == result

    @pytest.mark.parametrize(
        "functions,initial,result",
        [
            ((lambda x: x,), 12, 12),
            (
                (
                    lambda x: x + 10,
                    lambda x: x - 10,
                ),
                12,
                12,
            ),
            ((lambda x: bool(x),), 12, True),
        ],
    )
    def test_compose(self, functions, initial, result):
        pipeline = compose(*functions)
        assert pipeline(initial) == result

    @pytest.mark.parametrize(
        "functions,initial,result",
        [
            ((lambda x: x,), 12, 12),
            (
                (
                    lambda x: x + 10,
                    lambda x: x - 10,
                ),
                12,
                12,
            ),
            ((lambda x: bool(x),), 12, True),
        ],
    )
    def test_interchangability(self, functions, initial, result):
        result_pipe = pipe(initial, *functions)
        result_compose = compose(*functions)(initial)
        assert result_pipe == result
        assert result_compose == result
