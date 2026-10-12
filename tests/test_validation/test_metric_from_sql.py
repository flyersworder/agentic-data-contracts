"""`validate_metric_from_sql`: the CI gate for metrics that declare a FROM (#136).

With `from_sql` declared, `SELECT <sql_expression> FROM <from_sql>` is a whole
query, so what a one-table metric never needed can now be checked statically:
the FROM must parse, read only tables the contract governs, and define every
alias the expression qualifies a column with.
"""

from __future__ import annotations

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.semantic.base import MetricDefinition
from agentic_data_contracts.validation import validate_metric_from_sql

JOIN = "main.payments p JOIN main.fees f ON f.card_scheme = p.card_scheme"


def _contract(*tables: str) -> DataContract:
    listed = ", ".join(tables)
    return DataContract.from_yaml_string(
        f"""
version: "1.0"
name: from-sql-test
semantic:
  allowed_tables:
    - schema: main
      tables: [{listed}]
  forbidden_operations: [DELETE, DROP]
  rules: []
"""
    )


def _metric(
    expr: str = "f.fixed_amount + f.rate * p.eur_amount / 10000",
    from_sql: str | None = JOIN,
) -> MetricDefinition:
    return MetricDefinition(
        name="transaction_fee",
        description="Fee for one payment under one rule",
        sql_expression=expr,
        source_model="main.fees",
        from_sql=from_sql,
    )


def test_a_governed_join_with_defined_aliases_passes() -> None:
    assert validate_metric_from_sql(_contract("payments", "fees"), [_metric()]) == []


def test_a_metric_without_from_sql_is_not_checked() -> None:
    metric = _metric(expr="anything at all (", from_sql=None)
    assert validate_metric_from_sql(_contract("payments"), [metric]) == []


def test_an_ungoverned_table_is_reported() -> None:
    problems = validate_metric_from_sql(_contract("payments"), [_metric()])
    assert len(problems) == 1
    assert "transaction_fee" in problems[0]
    assert "main.fees" in problems[0]


def test_an_alias_the_from_does_not_define_is_reported() -> None:
    metric = _metric(expr="f.fixed_amount + m.rate")
    problems = validate_metric_from_sql(_contract("payments", "fees"), [metric])
    assert len(problems) == 1
    assert "'m'" in problems[0]


def test_a_table_name_counts_as_a_qualifier() -> None:
    metric = _metric(
        expr="fees.fixed_amount + payments.eur_amount",
        from_sql=(
            "main.payments JOIN main.fees ON fees.card_scheme = payments.card_scheme"
        ),
    )
    assert validate_metric_from_sql(_contract("payments", "fees"), [metric]) == []


def test_an_unparseable_from_is_reported() -> None:
    metric = _metric(from_sql="main.payments p JOIN ON")
    problems = validate_metric_from_sql(_contract("payments", "fees"), [metric])
    assert len(problems) == 1
    assert "cannot be parsed" in problems[0]


def test_a_second_statement_is_refused() -> None:
    metric = _metric(from_sql="main.payments p; DELETE FROM main.payments")
    problems = validate_metric_from_sql(_contract("payments", "fees"), [metric])
    assert len(problems) == 1
    assert "cannot be parsed" in problems[0]


def test_a_subquery_in_the_from_is_governed_too() -> None:
    metric = _metric(
        expr="s.n",
        from_sql="(SELECT COUNT(*) AS n FROM main.secrets) s",
    )
    problems = validate_metric_from_sql(_contract("payments"), [metric])
    assert any("main.secrets" in p for p in problems)


def test_caller_principal_checks_against_that_callers_tables() -> None:
    contract = DataContract.from_yaml_string(
        """
version: "1.0"
name: from-sql-principals
semantic:
  allowed_tables:
    - schema: main
      tables: [payments]
    - schema: main
      tables: [fees]
      allowed_principals: [finance]
  forbidden_operations: [DELETE]
  rules: []
"""
    )
    assert validate_metric_from_sql(contract, [_metric()]) == []
    problems = validate_metric_from_sql(contract, [_metric()], caller_principal="ops")
    assert len(problems) == 1
    assert "main.fees" in problems[0]
    assert (
        validate_metric_from_sql(contract, [_metric()], caller_principal="finance")
        == []
    )
