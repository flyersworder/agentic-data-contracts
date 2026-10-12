"""`validate_metric_from_sql`: the CI gate for metrics that declare a FROM (#136).

With `from_sql` declared, `SELECT <sql_expression> FROM <from_sql>` is a whole
query, so what a one-table metric never needed can now be checked statically:
the contract's own Validator must not refuse it, and the FROM must read the
metric's source_model. With an ExplainAdapter, the database must also bind
every name in it.
"""

from __future__ import annotations

import pytest

from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.semantic.base import MetricDefinition
from agentic_data_contracts.validation import (
    Validator,
    metric_query,
    validate_metric_from_sql,
)
from agentic_data_contracts.validation.explain import ExplainResult

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
    filters: list[str] | None = None,
) -> MetricDefinition:
    return MetricDefinition(
        name="transaction_fee",
        description="Fee for one payment under one rule",
        sql_expression=expr,
        source_model=source_model,
        from_sql=from_sql,
        filters=filters or [],
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


# --- The metric's filters are part of the query ----------------------------


def test_the_query_carries_the_metrics_filters() -> None:
    metric = _metric(filters=["p.status = 'ok'", "f.active"])
    query = metric_query(metric) or ""
    assert query.startswith(f"SELECT {metric.sql_expression}\nFROM {JOIN}\nWHERE ")
    assert "p.status = 'ok'" in query and "f.active" in query
    # A trailing `--` comment in a filter cannot swallow the rest of the query.
    commented = _metric(filters=["p.status = 'ok' -- settled only", "f.active"])
    assert validate_metric_from_sql(_contract("payments", "fees"), [commented]) == []


def test_a_filter_is_governed_too() -> None:
    metric = _metric(filters=["p.merchant IN (SELECT s.merchant FROM main.secrets s)"])
    problems = validate_metric_from_sql(_contract("payments", "fees"), [metric])
    assert len(problems) == 1
    assert "main.secrets" in problems[0]


# --- With a database, every name must bind ---------------------------------


@pytest.fixture
def adapter() -> DuckDBAdapter:
    db = DuckDBAdapter(":memory:")
    db.connection.execute(
        """
        CREATE TABLE main.payments (
            card_scheme VARCHAR, eur_amount DOUBLE,
            info STRUCT(amount DOUBLE)
        );
        CREATE TABLE main.fees (
            card_scheme VARCHAR, fixed_amount DOUBLE, rate DOUBLE
        );
        """
    )
    return db


def test_a_declared_join_that_binds_passes(adapter: DuckDBAdapter) -> None:
    contract = _contract("payments", "fees")
    assert (
        validate_metric_from_sql(contract, [_metric()], explain_adapter=adapter) == []
    )


@pytest.mark.parametrize(
    "expr",
    [
        "f.fixed_amount + m.rate",  # an alias the FROM does not define
        "f.fixed_amount + payments.eur_amount",  # a bare name after its alias
        "f.no_such_column",
    ],
)
def test_a_name_the_database_cannot_bind_is_reported(
    adapter: DuckDBAdapter, expr: str
) -> None:
    problems = validate_metric_from_sql(
        _contract("payments", "fees"), [_metric(expr=expr)], explain_adapter=adapter
    )
    assert len(problems) == 1
    assert "transaction_fee" in problems[0]


@pytest.mark.parametrize(
    "expr",
    [
        "SUM(info.amount)",  # a struct field, unqualified
        "SUM(p.info.amount)",  # a struct field, qualified
    ],
)
def test_struct_fields_bind(adapter: DuckDBAdapter, expr: str) -> None:
    metric = _metric(expr=expr, from_sql="main.payments p", source_model="")
    assert (
        validate_metric_from_sql(
            _contract("payments"), [metric], explain_adapter=adapter
        )
        == []
    )


def test_without_a_database_names_are_not_bound() -> None:
    # The static gate cannot tell `info.amount` (a struct field) from
    # `alias.column` without the schema, so it leaves binding to EXPLAIN.
    metric = _metric(expr="f.fixed_amount + m.rate")
    assert validate_metric_from_sql(_contract("payments", "fees"), [metric]) == []


class _RecordingExplain:
    def __init__(self) -> None:
        self.seen: list[str] = []

    def explain(self, sql: str) -> ExplainResult:
        self.seen.append(sql)
        return ExplainResult(
            estimated_cost_usd=1e9, estimated_rows=10**12, schema_valid=True
        )


def test_explain_runs_only_on_a_join_the_validator_admits() -> None:
    explain = _RecordingExplain()
    metric = _metric()
    validate_metric_from_sql(_contract("payments"), [metric], explain_adapter=explain)
    assert explain.seen == []
    validate_metric_from_sql(
        _contract("payments", "fees"), [metric], explain_adapter=explain
    )
    assert explain.seen == [metric_query(metric)]


def test_resource_limits_do_not_apply_to_a_bare_join() -> None:
    # A join with no WHERE estimates a full scan; the agent's query narrows it.
    contract = DataContract.from_yaml_string(
        """
version: "1.0"
name: from-sql-limits
semantic:
  allowed_tables:
    - schema: main
      tables: [payments, fees]
  forbidden_operations: [DELETE]
  rules: []
resources:
  cost_limit_usd: 1.0
  max_rows_scanned: 1000
"""
    )
    problems = validate_metric_from_sql(
        contract, [_metric()], explain_adapter=_RecordingExplain()
    )
    assert problems == []


class _PlusNormalizer:
    """A dialect whose `PLUS` operator sqlglot reads only after normalization."""

    def normalize_sql(self, sql: str) -> str:
        return sql.replace(" PLUS ", " + ")


def test_the_sql_normalizer_is_applied() -> None:
    metric = _metric(expr="f.fixed_amount PLUS p.eur_amount")
    contract = _contract("payments", "fees")
    assert (
        validate_metric_from_sql(contract, [metric], sql_normalizer=_PlusNormalizer())
        == []
    )
    problems = validate_metric_from_sql(contract, [metric])
    assert len(problems) == 1
    assert "parse error" in problems[0]


# --- The FROM reads the metric's own table ---------------------------------


def test_a_from_that_omits_the_source_model_is_reported() -> None:
    metric = _metric(expr="SUM(p.eur_amount)", from_sql="main.payments p")
    problems = validate_metric_from_sql(_contract("payments", "fees"), [metric])
    assert len(problems) == 1
    assert "main.fees" in problems[0]
    assert "source_model" in problems[0]


@pytest.mark.parametrize(
    ("source_model", "from_sql"),
    [
        ("fees", "main.fees f"),  # source_model without its schema
        ("prod.main.fees", "prod.main.fees f"),  # with its catalog
        ("MAIN.FEES", "main.fees f"),
    ],
)
def test_the_source_model_matches_however_it_is_qualified(
    source_model: str, from_sql: str
) -> None:
    metric = _metric(
        expr="SUM(f.fixed_amount)", from_sql=from_sql, source_model=source_model
    )
    assert validate_metric_from_sql(_contract("fees"), [metric]) == []


def test_a_metric_without_a_source_model_is_not_held_to_one() -> None:
    metric = _metric(
        expr="SUM(p.eur_amount)", from_sql="main.payments p", source_model=""
    )
    assert validate_metric_from_sql(_contract("payments"), [metric]) == []


# --- from_sql is a FROM clause, and only that ------------------------------


@pytest.mark.parametrize(
    "from_sql",
    [
        "main.fees f WHERE f.active",
        "main.fees f GROUP BY f.card_scheme",
        "main.fees f GROUP BY f.card_scheme HAVING COUNT(*) > 1",
        "main.fees f ORDER BY f.rate",
        "main.fees f LIMIT 1",
        "main.fees f UNION ALL SELECT 1 FROM main.payments",
    ],
)
def test_a_from_sql_that_is_more_than_a_from_clause_is_reported(
    from_sql: str,
) -> None:
    # The agent writes its own WHERE, GROUP BY and LIMIT around the join.
    metric = _metric(expr="SUM(f.rate)", from_sql=from_sql)
    problems = validate_metric_from_sql(_contract("payments", "fees"), [metric])
    assert len(problems) == 1
    assert "only a FROM clause" in problems[0]


def test_a_where_in_from_sql_is_named_even_with_filters() -> None:
    metric = _metric(
        expr="SUM(f.rate)", from_sql="main.fees f WHERE f.active", filters=["f.x"]
    )
    problems = validate_metric_from_sql(_contract("fees"), [metric])
    assert len(problems) == 1
    assert "only a FROM clause" in problems[0]


def test_a_blank_from_sql_is_reported_as_blank() -> None:
    metric = _metric(from_sql="   ")
    problems = validate_metric_from_sql(_contract("payments", "fees"), [metric])
    assert problems == ["metric 'transaction_fee' from_sql is blank"]


def test_a_source_model_read_only_by_a_filter_is_not_read_by_the_from() -> None:
    metric = _metric(
        expr="SUM(p.eur_amount)",
        from_sql="main.payments p",
        filters=["p.card_scheme IN (SELECT card_scheme FROM main.fees)"],
    )
    problems = validate_metric_from_sql(_contract("payments", "fees"), [metric])
    assert len(problems) == 1
    assert "source_model" in problems[0]


def test_a_quoted_source_model_matches() -> None:
    metric = _metric(
        expr="SUM(f.rate)", from_sql="main.fees f", source_model='"main"."fees"'
    )
    assert validate_metric_from_sql(_contract("fees"), [metric]) == []


def test_a_cte_named_like_the_source_model_does_not_count() -> None:
    metric = _metric(
        expr="SUM(f.eur_amount)",
        from_sql="(WITH fees AS (SELECT * FROM main.payments) SELECT * FROM fees) f",
        source_model="fees",
    )
    problems = validate_metric_from_sql(_contract("payments"), [metric])
    assert len(problems) == 1
    assert "source_model" in problems[0]
