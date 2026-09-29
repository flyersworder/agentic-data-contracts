"""Assemble one self-contained metric expression from a source's parts.

dbt and Cube keep a metric's aggregation, its column expression and its
filters in separate fields. ``assemble`` joins them into the single
``sql_expression`` every other layer expects, by templating, never by
generating SQL from an AST, so the result is engine-native text for any
dialect -- including ones sqlglot can parse but not emit, such as Denodo VQL.

A filter folds *inside* the aggregate -- ``SUM(CASE WHEN <f> THEN <e> END)``
-- rather than into a WHERE clause the expression cannot carry. For every
aggregation here that computes what ``WHERE <f>`` would: rows failing the
filter contribute NULL, which SUM/AVG/MIN/MAX/COUNT all skip, and a filter
no row passes gives NULL (COUNT: 0), as an empty WHERE does. dbt counts are
assembled as ``sum_boolean`` over ``e IS NOT NULL`` instead, which is what
MetricFlow compiles ``count`` to, so they are NULL there too. ``CASE`` rather
than ``FILTER (WHERE ...)`` because every supported engine has it.
"""

from __future__ import annotations

_AGGREGATES = {
    "sum": "SUM({})",
    "count": "COUNT({})",
    "count_distinct": "COUNT(DISTINCT {})",
    "avg": "AVG({})",
    "min": "MIN({})",
    "max": "MAX({})",
}

#: Aggregations ``assemble`` can express; ``sum_boolean`` is SUM over 1/0.
AGGREGATIONS = frozenset(_AGGREGATES) | {"sum_boolean"}


def assemble(agg: str, expr: str | None, conditions: list[str]) -> str:
    """``agg`` over ``expr``, counting only rows where every condition holds.

    ``expr`` None means "every row" and is only meaningful for ``count``.
    Raises ``ValueError`` for an aggregation outside ``AGGREGATIONS``; callers
    check first and record the reason instead.
    """
    if agg not in AGGREGATIONS:
        raise ValueError(f"no portable SQL for aggregation {agg!r}")
    if expr is None:
        if agg != "count":
            raise ValueError(f"aggregation {agg!r} needs an expression")
        expr = "1" if conditions else "*"
    if agg == "sum_boolean":
        agg, expr = "sum", f"CASE WHEN {expr} THEN 1 ELSE 0 END"
    if conditions:
        condition = (
            conditions[0]
            if len(conditions) == 1
            else " AND ".join(f"({c})" for c in conditions)
        )
        expr = f"CASE WHEN {condition} THEN {expr} END"
    return _AGGREGATES[agg].format(expr)
