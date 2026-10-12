"""`validate_metric_from_sql`: the CI gate for metrics that declare a FROM (#136).

With `from_sql` declared, `SELECT <sql_expression> FROM <from_sql>` is a whole
query, so what a one-table metric never needed can now be checked statically:
the contract's own Validator must not refuse it, every alias the expression
qualifies a column with must be visible where it is used, and the FROM must
read the metric's source_model.
"""

from __future__ import annotations

import pytest

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.semantic.base import MetricDefinition
from agentic_data_contracts.validation import (
    Validator,
    metric_query,
    validate_metric_from_sql,
)

JOIN = "main.payments p JOIN main.fees f ON f.card_scheme = p.card_scheme"


def _contract(*tables: str, rules: str = "[]") -> DataContract:
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
  rules: {rules}
"""
    )


def _metric(
    expr: str = "f.fixed_amount + f.rate * p.eur_amount / 10000",
    from_sql: str | None = JOIN,
    source_model: str = "main.fees",
) -> MetricDefinition:
    return MetricDefinition(
        name="transaction_fee",
        description="Fee for one payment under one rule",
        sql_expression=expr,
        source_model=source_model,
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
    assert "parse error" in problems[0]


def test_a_second_statement_is_refused() -> None:
    metric = _metric(from_sql="main.payments p; DELETE FROM main.payments")
    problems = validate_metric_from_sql(_contract("payments", "fees"), [metric])
    assert len(problems) == 1
    assert "multiple statements" in problems[0]


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
    # Omitted, the caller is anonymous -- as for the Validator at run time --
    # so a join over a principal-restricted table is refused.
    problems = validate_metric_from_sql(contract, [_metric()])
    assert len(problems) == 1
    assert "main.fees" in problems[0]
    problems = validate_metric_from_sql(contract, [_metric()], caller_principal="ops")
    assert len(problems) == 1
    assert "main.fees" in problems[0]
    assert (
        validate_metric_from_sql(contract, [_metric()], caller_principal="finance")
        == []
    )


# --- The join is judged by the contract's own Validator --------------------


@pytest.mark.parametrize(
    "from_sql",
    [
        "main.payments p",
        "MAIN.PAYMENTS p",  # the run-time allowlist compares names as written
        "payments p",  # no schema: not a table the allowlist names
        "main.payments p JOIN main.secrets s ON s.k = p.k",
    ],
)
def test_the_gate_refuses_exactly_what_the_run_time_validator_refuses(
    from_sql: str,
) -> None:
    contract = _contract("payments")
    metric = _metric(expr="SUM(p.eur_amount)", from_sql=from_sql, source_model="")
    run_time = Validator(contract).validate(metric_query(metric) or "")
    problems = validate_metric_from_sql(contract, [metric])
    assert bool(problems) == run_time.blocked
    assert problems == [
        f"metric 'transaction_fee' from_sql: {reason}" for reason in run_time.reasons
    ]


def test_a_blocked_column_in_the_expression_is_reported() -> None:
    rules = """
    - name: no_card_numbers
      description: card numbers stay out
      enforcement: block
      query_check:
        blocked_columns: [card_number]
"""
    metric = _metric(expr="COUNT(DISTINCT p.card_number)")
    problems = validate_metric_from_sql(
        _contract("payments", "fees", rules=rules), [metric]
    )
    assert len(problems) == 1
    assert "card_number" in problems[0]


def test_a_join_over_max_joins_is_reported() -> None:
    rules = """
    - name: few_joins
      description: keep joins small
      enforcement: block
      query_check:
        max_joins: 0
"""
    problems = validate_metric_from_sql(
        _contract("payments", "fees", rules=rules), [_metric()]
    )
    assert len(problems) == 1
    assert "join" in problems[0].lower()


def test_rules_the_agent_satisfies_by_what_it_adds_do_not_apply() -> None:
    # A required filter and a LIMIT are what the agent's query adds around the
    # declared join; the bare join cannot carry them, and must not fail for it.
    rules = """
    - name: tenant_filter
      description: always filter by tenant
      enforcement: block
      query_check:
        required_filter: tenant_id
    - name: tenant_values
      description: only your own tenant
      enforcement: block
      table: main.payments
      query_check:
        required_filter_values:
          column: tenant_id
          values_by_principal: {}
    - name: bounded
      description: always LIMIT
      enforcement: block
      query_check:
        require_limit: true
        no_select_star: true
"""
    contract = _contract("payments", "fees", rules=rules)
    assert validate_metric_from_sql(contract, [_metric()]) == []
    # ...while the contract itself is untouched.
    assert Validator(contract).validate(metric_query(_metric()) or "").blocked


def test_a_warn_rule_does_not_fail_the_gate() -> None:
    rules = """
    - name: few_joins
      description: keep joins small
      enforcement: warn
      query_check:
        max_joins: 0
"""
    contract = _contract("payments", "fees", rules=rules)
    assert validate_metric_from_sql(contract, [_metric()]) == []


# --- Qualifiers resolve the way the database resolves them -----------------


@pytest.mark.parametrize(
    ("expr", "from_sql"),
    [
        ("SUM(p.info.amount)", "main.payments p"),  # struct field access
        ("SUM(main.payments.eur_amount)", "main.payments"),  # schema-qualified
        ("SUM(v.x)", "main.payments p JOIN (VALUES (1)) AS v(x) ON TRUE"),
        ("SUM(u.x)", "main.payments p, UNNEST(p.arr) AS u(x)"),
        ("SUM(l.n)", "main.payments p, LATERAL (SELECT 1 AS n) AS l"),
        (
            "(SELECT MAX(f.fixed_amount) FROM main.fees f WHERE f.k = p.k)",
            "main.payments p",
        ),  # correlated: p is the outer query's
    ],
)
def test_qualifiers_the_database_resolves_pass(expr: str, from_sql: str) -> None:
    metric = _metric(expr=expr, from_sql=from_sql, source_model="main.payments")
    assert (
        validate_metric_from_sql(
            _contract("payments", "fees"), [metric], dialect="duckdb"
        )
        == []
    )


def test_a_tables_bare_name_does_not_qualify_once_it_is_aliased() -> None:
    metric = _metric(
        expr="SUM(payments.eur_amount)",
        from_sql="main.payments p",
        source_model="main.payments",
    )
    problems = validate_metric_from_sql(_contract("payments"), [metric])
    assert len(problems) == 1
    assert "'payments'" in problems[0]


def test_an_alias_inside_a_subquery_is_not_visible_outside_it() -> None:
    metric = _metric(
        expr="SUM(s2.fixed_amount)",
        from_sql=(
            "main.payments p JOIN (SELECT fixed_amount FROM main.fees s2) s ON TRUE"
        ),
    )
    problems = validate_metric_from_sql(_contract("payments", "fees"), [metric])
    assert len(problems) == 1
    assert "'s2'" in problems[0]


class _PlusNormalizer:
    """A dialect whose `PLUS` operator sqlglot reads only after normalization."""

    def normalize_sql(self, sql: str) -> str:
        return sql.replace(" PLUS ", " + ")


def test_qualifiers_are_checked_after_normalization() -> None:
    metric = _metric(expr="f.fixed_amount PLUS m.rate")
    problems = validate_metric_from_sql(
        _contract("payments", "fees"), [metric], sql_normalizer=_PlusNormalizer()
    )
    assert len(problems) == 1
    assert "'m'" in problems[0]


# --- The FROM reads the metric's own table ---------------------------------


def test_a_from_that_omits_the_source_model_is_reported() -> None:
    metric = _metric(expr="SUM(p.eur_amount)", from_sql="main.payments p")
    problems = validate_metric_from_sql(_contract("payments", "fees"), [metric])
    assert len(problems) == 1
    assert "main.fees" in problems[0]
    assert "source_model" in problems[0]


def test_a_metric_without_a_source_model_is_not_held_to_one() -> None:
    metric = _metric(
        expr="SUM(p.eur_amount)", from_sql="main.payments p", source_model=""
    )
    assert validate_metric_from_sql(_contract("payments"), [metric]) == []
