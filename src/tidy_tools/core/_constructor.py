from typing import Callable, _LiteralGenericAlias
import functools
import operator

from pyspark.sql import Column, functions as F

from tidy_tools.core._types import ColumnReference


def construct_query(
    *columns: ColumnReference,
    predicate: Callable,
    strict: bool = False,
    invert: bool = False,
    **kwargs,
) -> Column:
    """Factory to generate generic filtering queries"""
    compare_operator = operator.and_ if strict else operator.or_
    predicate = functools.partial(predicate, **kwargs)
    query = functools.reduce(compare_operator, map(predicate, columns))
    return operator.inv(query) if invert else query


def construct_if_else(
    predicate_mapping: dict[_LiteralGenericAlias, dict],
    strict: bool = True,
    **kwargs,
) -> Column:
    """Factory to generate generic if-else expression."""

    def generate_expressions(
        predicate_mapping: dict[_LiteralGenericAlias, dict],
    ) -> dict[_LiteralGenericAlias, Column]:
        """Convert predicate mapping values into column expressions."""
        compare_operator = operator.and_ if strict else operator.or_
        return {
            label: functools.reduce(
                compare_operator,
                map(
                    lambda item: F.col(item[0]) == F.lit(item[1]),
                    params.items(),
                ),
            )
            for label, params in predicate_mapping.items()
        }

    def reduce_expressions(expressions: dict) -> Column:
        conditions = list((key, cond) for key, cond in expressions.items())
        return functools.reduce(
            lambda acc, cond: acc.when(cond[1], F.lit(cond[0])),
            conditions[1:],
            F.when(conditions[0][1], F.lit(conditions[0][0])),
        )

    unique_mappings = len(set(pair.values() for pair in predicate_mapping.values()))
    assert len(predicate_mapping.values()) == len(
        unique_mappings
    ), "Non-unique values discovered."
    return reduce_expressions(generate_expressions(predicate_mapping))
