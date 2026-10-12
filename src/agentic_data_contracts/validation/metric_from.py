"""CI gate for metrics that declare the FROM clause they are written against (#136).

A metric's ``sql_expression`` may read several tables through aliases; its
``from_sql`` declares that join. Together they make a whole query,
``SELECT <sql_expression> FROM <from_sql>``, so three things can be checked
without a database:

- the query parses, as one statement;
- every table it reads is one the contract governs, as a shadow's must be
  (``validate_sensitivity_tables``) -- a declared join is contract-authored
  SQL the agent is told to reuse, and must not reach data no query may;
- every qualifier on a column of ``sql_expression`` is an alias or table
  name the FROM defines, so the expression and its join agree.

Like ``validate_sensitivity_tables`` this is a CI-time gate, not a load-time
one: ``YamlSource`` holds no ``DataContract`` and no dialect. Whether the
query RUNS, and what it means, needs a database and a reviewer.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import sqlglot
from sqlglot import exp

from agentic_data_contracts.adapters._normalizer import SqlNormalizer
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.validation.checkers import extract_tables
from agentic_data_contracts.validation.sensitivity import (
    Normalize,
    _normalizer_fn,
    _why_ungoverned,
)
from agentic_data_contracts.validation.validator import _is_multi_statement

if TYPE_CHECKING:
    from agentic_data_contracts.semantic.base import MetricDefinition


def metric_query(metric: MetricDefinition) -> str | None:
    """``SELECT <sql_expression> FROM <from_sql>``, or None without a FROM.

    The query a declared join makes of the metric. A metric whose expression
    is empty (an ``untranslated`` one) selects ``1``, so its FROM is still
    checkable.
    """
    if metric.from_sql is None:
        return None
    expression = metric.sql_expression.strip() or "1"
    return f"SELECT {expression}\nFROM {metric.from_sql}"


def validate_metric_from_sql(
    contract: DataContract,
    metrics: list[MetricDefinition],
    *,
    dialect: str | None = None,
    sql_normalizer: SqlNormalizer | None = None,
    caller_principal: str | None = None,
) -> list[str]:
    """Problems with the metrics' declared FROM clauses; empty when none.

    Metrics without ``from_sql`` are skipped. ``dialect``,
    ``sql_normalizer`` and ``caller_principal`` mean what they mean for
    ``validate_sensitivity_tables``: omitted, the table check is structural,
    against every table the contract declares.
    """
    declared = {name.lower() for name in contract.allowed_table_names()}
    allowed = {
        name.lower()
        for name in (
            contract.allowed_table_names()
            if caller_principal is None
            else contract.allowed_table_names_for(caller_principal)
        )
    }
    normalize = _normalizer_fn(sql_normalizer)
    problems: list[str] = []
    for metric in metrics:
        query = metric_query(metric)
        if query is None:
            continue
        where = f"metric {metric.name!r} from_sql"
        tree = _parse(query, dialect=dialect, normalize=normalize)
        if tree is None:
            problems.append(
                f"{where} cannot be parsed together with its sql_expression, "
                "so its tables cannot be checked against the contract"
            )
            continue
        for name in sorted(extract_tables(tree)):
            if name.lower() not in allowed:
                problems.append(
                    f"{where} reads {name!r}, which "
                    f"{_why_ungoverned(name, declared, caller_principal)}"
                )
        for qualifier in sorted(_undefined_qualifiers(tree, metric, dialect)):
            problems.append(
                f"metric {metric.name!r}: sql_expression qualifies a column with "
                f"{qualifier!r}, which from_sql does not define"
            )
    return problems


def _parse(
    query: str, *, dialect: str | None, normalize: Normalize
) -> exp.Expression | None:
    try:
        text = normalize(query)
        if _is_multi_statement(text, dialect):
            return None
        parsed = sqlglot.parse_one(text, dialect=dialect)
    except Exception:  # noqa: BLE001 - every failure means "cannot be checked"
        return None
    return None if parsed is None else cast(exp.Expression, parsed)


def _undefined_qualifiers(
    tree: exp.Expression, metric: MetricDefinition, dialect: str | None
) -> set[str]:
    """Qualifiers on ``sql_expression``'s columns that name nothing in the FROM.

    Defined names are every table's alias and bare name, and every aliased
    subquery, anywhere in the query -- a correlated subquery inside the
    expression may define its own.
    """
    defined: set[str] = set()
    for table in tree.find_all(exp.Table):
        defined.add(table.name.lower())
        if table.alias:
            defined.add(table.alias.lower())
    for sub in tree.find_all(exp.Subquery):
        if sub.alias:
            defined.add(sub.alias.lower())
    try:
        expression = sqlglot.parse_one(metric.sql_expression or "1", dialect=dialect)
    except Exception:  # noqa: BLE001 - the whole query parsed; be lenient here
        return set()
    return {
        column.table
        for column in expression.find_all(exp.Column)
        if column.table and column.table.lower() not in defined
    }
