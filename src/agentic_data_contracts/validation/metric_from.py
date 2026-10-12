"""CI gate for metrics that declare the FROM clause they are written against (#136).

A metric's ``sql_expression`` may read several tables through aliases; its
``from_sql`` declares that join. Together they make a whole query,
``SELECT <sql_expression> FROM <from_sql>``, so three things can be checked
without a database:

- the contract's own ``Validator`` does not refuse it -- a declared join is
  contract-authored SQL the agent is told to reuse, and the Validator is what
  will judge that reuse, so the gate asks it rather than re-deriving its
  checks. The rules an agent satisfies by what it ADDS around the join (a
  required filter, a LIMIT) are left out: the bare join cannot carry them;
- every qualifier on a column names a table or alias visible where the column
  is used, resolved scope by scope the way the database resolves it;
- the FROM reads the metric's ``source_model``, so lookups that find metrics
  by table point at a join that reads it.

Like ``validate_sensitivity_tables`` this is a CI-time gate, not a load-time
one: ``YamlSource`` holds no ``DataContract`` and no dialect. Whether the
query RUNS, and what it means, needs a database and a reviewer.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlglot import exp
from sqlglot.optimizer.scope import Scope, traverse_scope

from agentic_data_contracts.adapters._normalizer import SqlNormalizer
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.principal import Principal
from agentic_data_contracts.validation.checkers import extract_tables
from agentic_data_contracts.validation.sensitivity import (
    _normalizer_fn,
    _parse_statement,
)
from agentic_data_contracts.validation.validator import Validator

if TYPE_CHECKING:
    from agentic_data_contracts.semantic.base import MetricDefinition

#: Query checks an agent satisfies by what its query adds around a declared
#: join -- a WHERE filter, a LIMIT. The bare join cannot carry them.
_ADDED_BY_THE_AGENT = {
    "required_filter": None,
    "required_filter_values": None,
    "require_limit": None,
}


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
    caller_principal: Principal = None,
) -> list[str]:
    """Problems with the metrics' declared FROM clauses; empty when none.

    Metrics without ``from_sql`` are skipped. ``dialect``, ``sql_normalizer``
    and ``caller_principal`` mean what they mean for ``Validator``, and should
    match what the agent's queries will be validated with. In particular the
    default ``caller_principal=None`` is an anonymous caller, denied every
    principal-restricted table: pass the principal that will reuse the metric.
    """
    validator = Validator(
        _without_rules_the_agent_satisfies(contract),
        dialect=dialect,
        sql_normalizer=sql_normalizer,
        caller_principal=caller_principal,
    )
    normalize = _normalizer_fn(sql_normalizer)
    problems: list[str] = []
    for metric in metrics:
        query = metric_query(metric)
        if query is None:
            continue
        where = f"metric {metric.name!r} from_sql"
        try:
            result = validator.validate(query)
        except Exception as e:  # noqa: BLE001 - a normalizer may raise anything
            problems.append(f"{where}: cannot be checked: {e}")
            continue
        if result.blocked:
            problems.extend(f"{where}: {reason}" for reason in result.reasons)
        tree = _parse_statement(query, dialect=dialect, normalize=normalize)
        if tree is None:
            continue  # the Validator has already reported why
        try:
            undefined = _undefined_qualifiers(tree)
        except Exception as e:  # noqa: BLE001 - sqlglot's scope builder can raise
            problems.append(f"{where}: column qualifiers cannot be checked: {e}")
            undefined = set()
        problems.extend(
            f"{where}: a column is qualified with {qualifier!r}, which names "
            "no table or alias visible where it is used"
            for qualifier in sorted(undefined)
        )
        source = metric.source_model.strip()
        read = {name.lower() for name in extract_tables(tree)}
        if source and source.lower() not in read:
            problems.append(
                f"{where} does not read the metric's source_model {source!r}"
            )
    return problems


def _without_rules_the_agent_satisfies(contract: DataContract) -> DataContract:
    """A copy of *contract* whose query checks a bare join can be held to."""
    semantic = contract.schema.semantic
    rules = [
        rule
        if rule.query_check is None
        else rule.model_copy(
            update={
                "query_check": rule.query_check.model_copy(update=_ADDED_BY_THE_AGENT)
            }
        )
        for rule in semantic.rules
    ]
    return DataContract(
        contract.schema.model_copy(
            update={"semantic": semantic.model_copy(update={"rules": rules})}
        )
    )


def _undefined_qualifiers(tree: exp.Expression) -> set[str]:
    """Column qualifiers that name no source visible in the column's scope.

    A scope sees its own sources and, for a correlated subquery, its
    enclosing scopes'. An aliased table is visible only by its alias, as in
    the database. A three-part reference (``p.info.amount``) is skipped:
    without a schema it may be a struct field as well as ``schema.table.col``.
    """
    undefined: set[str] = set()
    for scope in traverse_scope(tree):
        for column in scope.columns:
            if not column.table or column.args.get("db"):
                continue
            if not _visible(column.table, scope):
                undefined.add(column.table)
    return undefined


def _visible(qualifier: str, scope: Scope | None) -> bool:
    while scope is not None:
        if qualifier.lower() in {name.lower() for name in scope.sources}:
            return True
        scope = scope.parent
    return False
