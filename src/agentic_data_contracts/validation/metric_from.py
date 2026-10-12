"""CI gate for metrics that declare the FROM clause they are written against (#136).

A metric's ``sql_expression`` may read several tables through aliases; its
``from_sql`` declares that join. Together they make a whole query,
``SELECT <sql_expression> FROM <from_sql> WHERE <filters>``, and the gate
checks that:

- the contract's own ``Validator`` does not refuse it -- a declared join is
  contract-authored SQL the agent is told to reuse, and the Validator is what
  will judge that reuse, so the gate asks it rather than re-deriving its
  checks. The rules an agent satisfies by what it ADDS around the join (a
  required filter, a LIMIT) are left out: the bare join cannot carry them;
- the FROM reads the metric's ``source_model``, so lookups that find metrics
  by table point at a join that reads it;
- given an ``ExplainAdapter``, the database binds every name in it. Without
  one, names are not checked: without the schema, ``info.amount`` may be a
  struct field as well as ``alias.column``, and only the database knows.

Like ``validate_sensitivity_tables`` this is a CI-time gate, not a load-time
one: ``YamlSource`` holds no ``DataContract`` and no dialect. What the query
MEANS still needs a reviewer.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlglot import exp

from agentic_data_contracts.adapters._normalizer import SqlNormalizer
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.principal import Principal
from agentic_data_contracts.validation.explain import ExplainAdapter
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

    The query a declared join makes of the metric, with its ``filters`` as a
    WHERE clause: without them it is a different quantity. A metric whose
    expression is empty (an ``untranslated`` one) selects ``1``, so its FROM
    is still checkable.
    """
    if metric.from_sql is None:
        return None
    expression = metric.sql_expression.strip() or "1"
    query = f"SELECT {expression}\nFROM {metric.from_sql}"
    if metric.filters:
        # The newline keeps a trailing `--` comment in a filter from
        # swallowing the closing paren, as for a `{{ metric:NAME }}` expansion.
        query += "\nWHERE " + " AND ".join(f"({f}\n)" for f in metric.filters)
    return query


def validate_metric_from_sql(
    contract: DataContract,
    metrics: list[MetricDefinition],
    *,
    dialect: str | None = None,
    sql_normalizer: SqlNormalizer | None = None,
    caller_principal: Principal = None,
    explain_adapter: ExplainAdapter | None = None,
) -> list[str]:
    """Problems with the metrics' declared FROM clauses; empty when none.

    Metrics without ``from_sql`` are skipped. ``dialect``, ``sql_normalizer``
    and ``caller_principal`` mean what they mean for ``Validator``, and should
    match what the agent's queries will be validated with. In particular the
    default ``caller_principal=None`` is an anonymous caller, denied every
    principal-restricted table: pass the principal that will reuse the metric.

    ``explain_adapter`` (a database adapter, typically) runs EXPLAIN on each
    query the Validator admits, and reports what the database cannot bind --
    an alias the FROM does not define, a missing column. Only binding: the
    contract's cost and row limits are not applied, because a join with no
    WHERE estimates the full scan the agent's own query will narrow.
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
        elif explain_adapter is not None:
            problems.extend(f"{where}: {p}" for p in _binding(query, explain_adapter))
        tree = _parse_statement(query, dialect=dialect, normalize=normalize)
        if tree is None:
            continue  # the Validator has already reported why
        source = metric.source_model.strip()
        if source and not _reads(tree, source):
            problems.append(
                f"{where} does not read the metric's source_model {source!r}"
            )
    return problems


def _binding(query: str, explain_adapter: ExplainAdapter) -> list[str]:
    """What the database cannot bind in *query*; empty when it all binds."""
    try:
        explained = explain_adapter.explain(query)
    except Exception as e:  # noqa: BLE001 - an adapter may raise anything
        return [f"EXPLAIN failed: {e}"]
    if explained.schema_valid:
        return []
    return [f"the database cannot bind it: {'; '.join(explained.errors)}"]


def _reads(tree: exp.Expression, source_model: str) -> bool:
    """Whether *tree* reads *source_model*, however either is qualified.

    Names are compared part by part from the right, over the parts both
    spell out: ``fees`` and ``prod.main.fees`` both name ``main.fees``.
    """
    wanted = source_model.lower().split(".")
    for table in tree.find_all(exp.Table):
        parts = [p.lower() for p in (table.catalog, table.db, table.name) if p]
        n = min(len(parts), len(wanted))
        if parts[-n:] == wanted[-n:]:
            return True
    return False


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
