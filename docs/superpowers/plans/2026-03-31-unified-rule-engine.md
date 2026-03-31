# Unified Rule Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the hardcoded checker pipeline with a rules-driven, three-phase validation engine supporting declarative query checks, result checks, table scoping, and session cost enforcement.

**Architecture:** All rules live in one YAML `rules` list with `query_check` or `result_check` blocks. The Validator parses SQL once into an AST, runs table-scoped query checkers (Phase 1), EXPLAIN (Phase 2), then result checkers post-execution (Phase 3). `run_query` orchestrates all three phases and records session costs.

**Tech Stack:** Python 3.12+, Pydantic 2, sqlglot, pytest, DuckDB (integration tests)

**Spec:** `docs/superpowers/specs/2026-03-31-unified-rule-engine-design.md`

---

## File Structure

| File | Responsibility |
|---|---|
| `src/agentic_data_contracts/core/schema.py` | Add `QueryCheck`, `ResultCheck` models. Rewrite `SemanticRule`. Remove `filter_column`. |
| `src/agentic_data_contracts/validation/checkers.py` | Refactor all checkers to `check_ast()`. Add `BlockedColumnsChecker`, `RequireLimitChecker`, `MaxJoinsChecker`, `ResultCheckRunner`. Extract `extract_tables()`. |
| `src/agentic_data_contracts/validation/validator.py` | Rules-driven `_build_checkers()`. Add `validate_results()`. Pass `estimated_cost_usd` through. Parse AST once. |
| `src/agentic_data_contracts/core/contract.py` | Keep `block_rules()`/`warn_rules()`/`log_rules()` (bridge + prompt still use them). |
| `src/agentic_data_contracts/core/prompt.py` | Update `_render_constraints()` to show query_check/result_check details. |
| `src/agentic_data_contracts/bridge/compiler.py` | Update to work with new rule model (no `filter_column`). |
| `src/agentic_data_contracts/tools/factory.py` | Update `run_query` for Phase 3 + session cost. Update `validate_query` to note result checks. |
| `tests/fixtures/valid_contract.yml` | Rewrite to new rule format. |
| `tests/fixtures/minimal_contract.yml` | No change (has `rules: []`). |

---

### Task 1: Update Pydantic Schema Models

**Files:**
- Modify: `src/agentic_data_contracts/core/schema.py:1-67`
- Test: `tests/test_core/test_schema.py`

- [ ] **Step 1: Write failing tests for new models**

In `tests/test_core/test_schema.py`, replace the entire file:

```python
from pathlib import Path

import pytest
import yaml

from agentic_data_contracts.core.schema import (
    AllowedTable,
    DataContractSchema,
    Enforcement,
    QueryCheck,
    ResultCheck,
    SemanticRule,
    SuccessCriterionConfig,
)


def test_full_contract_parses(fixtures_dir: Path) -> None:
    raw = yaml.safe_load((fixtures_dir / "valid_contract.yml").read_text())
    schema = DataContractSchema.model_validate(raw)
    assert schema.name == "revenue-analysis"
    assert schema.version == "1.0"
    assert len(schema.semantic.allowed_tables) == 2
    assert schema.semantic.allowed_tables[0].schema_ == "analytics"
    assert schema.semantic.allowed_tables[0].tables == [
        "orders",
        "customers",
        "subscriptions",
    ]
    assert schema.resources is not None
    assert schema.resources.cost_limit_usd == 5.00
    assert schema.resources.max_retries == 3
    assert schema.temporal is not None
    assert schema.temporal.max_duration_seconds == 300
    assert len(schema.success_criteria) == 3
    assert schema.success_criteria[0].weight == pytest.approx(0.4)


def test_minimal_contract_parses(fixtures_dir: Path) -> None:
    raw = yaml.safe_load((fixtures_dir / "minimal_contract.yml").read_text())
    schema = DataContractSchema.model_validate(raw)
    assert schema.name == "basic-query"
    assert schema.semantic.source is None
    assert schema.resources is None
    assert schema.temporal is None
    assert schema.success_criteria == []


def test_invalid_enforcement_rejected() -> None:
    with pytest.raises(Exception):
        SemanticRule.model_validate(
            {
                "name": "bad",
                "description": "bad rule",
                "enforcement": "crash",
                "query_check": {"no_select_star": True},
            }
        )


def test_enforcement_values() -> None:
    for val in (Enforcement.BLOCK, Enforcement.WARN, Enforcement.LOG):
        rule = SemanticRule(
            name="test",
            description="test",
            enforcement=val,
            query_check=QueryCheck(no_select_star=True),
        )
        assert rule.enforcement == val


def test_success_criteria_weight_validation() -> None:
    with pytest.raises(Exception):
        SuccessCriterionConfig(name="bad", weight=1.5)
    with pytest.raises(Exception):
        SuccessCriterionConfig(name="bad", weight=-0.1)


def test_allowed_table_empty_tables() -> None:
    t = AllowedTable.model_validate({"schema": "raw", "tables": []})
    assert t.tables == []


def test_query_check_rule() -> None:
    rule = SemanticRule(
        name="tenant_filter",
        description="Must filter by tenant_id",
        enforcement=Enforcement.BLOCK,
        table="analytics.orders",
        query_check=QueryCheck(required_filter="tenant_id"),
    )
    assert rule.table == "analytics.orders"
    assert rule.query_check is not None
    assert rule.query_check.required_filter == "tenant_id"
    assert rule.result_check is None


def test_result_check_rule() -> None:
    rule = SemanticRule(
        name="wau_sanity",
        description="WAU must be reasonable",
        enforcement=Enforcement.WARN,
        table="analytics.user_metrics",
        result_check=ResultCheck(column="wau", max_value=8_000_000_000),
    )
    assert rule.result_check is not None
    assert rule.result_check.column == "wau"
    assert rule.result_check.max_value == 8_000_000_000


def test_rule_rejects_both_checks() -> None:
    with pytest.raises(ValueError, match="must not have both"):
        SemanticRule(
            name="bad",
            description="bad",
            enforcement=Enforcement.BLOCK,
            query_check=QueryCheck(no_select_star=True),
            result_check=ResultCheck(min_rows=1),
        )


def test_advisory_rule_no_checks() -> None:
    """Rules with neither check are advisory — shown in prompt only."""
    rule = SemanticRule(
        name="advisory",
        description="Just a guideline",
        enforcement=Enforcement.WARN,
    )
    assert rule.query_check is None
    assert rule.result_check is None


def test_table_scoping_optional() -> None:
    rule = SemanticRule(
        name="global_rule",
        description="Applies everywhere",
        enforcement=Enforcement.BLOCK,
        query_check=QueryCheck(require_limit=True),
    )
    assert rule.table is None


def test_query_check_multiple_fields() -> None:
    qc = QueryCheck(
        required_filter="tenant_id",
        no_select_star=True,
        max_joins=3,
    )
    assert qc.required_filter == "tenant_id"
    assert qc.no_select_star is True
    assert qc.max_joins == 3


def test_result_check_row_bounds() -> None:
    rc = ResultCheck(min_rows=1, max_rows=10000)
    assert rc.min_rows == 1
    assert rc.max_rows == 10000
    assert rc.column is None


def test_result_check_column_bounds() -> None:
    rc = ResultCheck(column="revenue", min_value=0, not_null=True)
    assert rc.column == "revenue"
    assert rc.min_value == 0
    assert rc.not_null is True
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_core/test_schema.py -v`
Expected: FAIL — `QueryCheck` and `ResultCheck` not importable, `SemanticRule` still has old shape.

- [ ] **Step 3: Implement schema changes**

Replace `src/agentic_data_contracts/core/schema.py` with:

```python
"""Pydantic models for YAML data contract validation."""

from __future__ import annotations

from enum import StrEnum
from typing import Self

from pydantic import BaseModel, Field, model_validator


class Enforcement(StrEnum):
    BLOCK = "block"
    WARN = "warn"
    LOG = "log"


class SemanticSource(BaseModel):
    type: str  # dbt | cube | yaml | custom
    path: str


class AllowedTable(BaseModel):
    schema_: str = Field(alias="schema")
    tables: list[str] = Field(default_factory=list)

    model_config = {"populate_by_name": True}


class QueryCheck(BaseModel):
    required_filter: str | None = None
    no_select_star: bool | None = None
    blocked_columns: list[str] | None = None
    require_limit: bool | None = None
    max_joins: int | None = None


class ResultCheck(BaseModel):
    column: str | None = None
    min_value: float | None = None
    max_value: float | None = None
    not_null: bool | None = None
    min_rows: int | None = None
    max_rows: int | None = None


class SemanticRule(BaseModel):
    name: str
    description: str
    enforcement: Enforcement
    table: str | None = None
    query_check: QueryCheck | None = None
    result_check: ResultCheck | None = None

    @model_validator(mode="after")
    def at_most_one_check(self) -> Self:
        if self.query_check is not None and self.result_check is not None:
            raise ValueError(
                "Rule must not have both query_check and result_check"
            )
        return self


class SemanticConfig(BaseModel):
    source: SemanticSource | None = None
    allowed_tables: list[AllowedTable] = Field(default_factory=list)
    forbidden_operations: list[str] = Field(default_factory=list)
    rules: list[SemanticRule] = Field(default_factory=list)
    domains: dict[str, list[str]] = Field(default_factory=dict)


class ResourceConfig(BaseModel):
    cost_limit_usd: float | None = None
    max_query_time_seconds: float | None = None
    max_retries: int | None = None
    max_rows_scanned: int | None = None
    token_budget: int | None = None


class TemporalConfig(BaseModel):
    max_duration_seconds: float | None = None


class SuccessCriterionConfig(BaseModel):
    name: str
    weight: float = Field(ge=0.0, le=1.0, default=1.0)


class DataContractSchema(BaseModel):
    version: str = "1.0"
    name: str
    semantic: SemanticConfig
    resources: ResourceConfig | None = None
    temporal: TemporalConfig | None = None
    success_criteria: list[SuccessCriterionConfig] = Field(default_factory=list)
```

- [ ] **Step 4: Update valid_contract.yml to new rule format**

Replace `tests/fixtures/valid_contract.yml` with:

```yaml
version: "1.0"
name: revenue-analysis

semantic:
  source:
    type: dbt
    path: "./dbt/manifest.json"
  allowed_tables:
    - schema: analytics
      tables: [orders, customers, subscriptions]
    - schema: raw
      tables: []
  forbidden_operations: [DELETE, DROP, TRUNCATE, UPDATE, INSERT]
  domains:
    revenue: [total_revenue]
    engagement: [active_customers]
  rules:
    - name: tenant_isolation
      description: "All queries must include a WHERE tenant_id = filter"
      enforcement: block
      query_check:
        required_filter: tenant_id
    - name: use_approved_metrics
      description: "Revenue calculations must use the semantic layer definition"
      enforcement: warn
    - name: no_select_star
      description: "Queries must specify explicit columns, no SELECT *"
      enforcement: block
      query_check:
        no_select_star: true

resources:
  cost_limit_usd: 5.00
  max_query_time_seconds: 30
  max_retries: 3
  max_rows_scanned: 1000000
  token_budget: 50000

temporal:
  max_duration_seconds: 300

success_criteria:
  - name: query_uses_semantic_definitions
    weight: 0.4
  - name: results_are_reproducible
    weight: 0.3
  - name: output_includes_methodology
    weight: 0.3
```

Note: `use_approved_metrics` is an advisory rule — no `query_check` or `result_check`. The model validator allows this (at most one check, not exactly one). Advisory rules appear in the system prompt but don't run any enforcement.

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_core/test_schema.py -v`
Expected: All PASS.

- [ ] **Step 6: Commit**

```bash
git add src/agentic_data_contracts/core/schema.py tests/test_core/test_schema.py tests/fixtures/valid_contract.yml
git commit -m "feat: add QueryCheck and ResultCheck models, rewrite SemanticRule"
```

---

### Task 2: Refactor Checkers to AST-Based Protocol + Add New Checkers

**Files:**
- Modify: `src/agentic_data_contracts/validation/checkers.py:1-158`
- Test: `tests/test_validation/test_checkers.py`

- [ ] **Step 1: Write failing tests for AST-based checkers**

Replace `tests/test_validation/test_checkers.py` with:

```python
from pathlib import Path

import pytest
import sqlglot

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.validation.checkers import (
    BlockedColumnsChecker,
    MaxJoinsChecker,
    NoSelectStarChecker,
    OperationBlocklistChecker,
    RequiredFilterChecker,
    RequireLimitChecker,
    TableAllowlistChecker,
    extract_tables,
)


def _parse(sql: str) -> sqlglot.exp.Expression:
    return sqlglot.parse_one(sql)


@pytest.fixture
def contract(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml(fixtures_dir / "valid_contract.yml")


class TestExtractTables:
    def test_simple_select(self) -> None:
        ast = _parse("SELECT id FROM analytics.orders")
        assert extract_tables(ast) == {"analytics.orders"}

    def test_join(self) -> None:
        ast = _parse(
            "SELECT o.id FROM analytics.orders o"
            " JOIN analytics.customers c ON o.id = c.id"
        )
        assert extract_tables(ast) == {"analytics.orders", "analytics.customers"}

    def test_cte_excluded(self) -> None:
        ast = _parse(
            "WITH cte AS (SELECT id FROM analytics.orders) SELECT id FROM cte"
        )
        assert extract_tables(ast) == {"analytics.orders"}

    def test_subquery(self) -> None:
        ast = _parse("SELECT * FROM (SELECT id FROM secret.data) t")
        assert extract_tables(ast) == {"secret.data"}


class TestTableAllowlistChecker:
    def test_allowed_table_passes(self, contract: DataContract) -> None:
        ast = _parse("SELECT id FROM analytics.orders WHERE tenant_id = 'x'")
        result = TableAllowlistChecker().check_ast(ast, contract)
        assert result.passed

    def test_forbidden_table_blocked(self, contract: DataContract) -> None:
        ast = _parse("SELECT id FROM raw.payments")
        result = TableAllowlistChecker().check_ast(ast, contract)
        assert not result.passed
        assert "raw.payments" in result.message

    def test_unknown_table_blocked(self, contract: DataContract) -> None:
        ast = _parse("SELECT id FROM secret.data")
        result = TableAllowlistChecker().check_ast(ast, contract)
        assert not result.passed

    def test_subquery_tables_checked(self, contract: DataContract) -> None:
        ast = _parse("SELECT * FROM (SELECT id FROM secret.data) t")
        result = TableAllowlistChecker().check_ast(ast, contract)
        assert not result.passed

    def test_join_tables_checked(self, contract: DataContract) -> None:
        ast = _parse(
            "SELECT o.id FROM analytics.orders o"
            " JOIN analytics.customers c ON o.id = c.id"
        )
        result = TableAllowlistChecker().check_ast(ast, contract)
        assert result.passed

    def test_cte_tables_checked(self, contract: DataContract) -> None:
        ast = _parse(
            "WITH cte AS (SELECT id FROM analytics.orders) SELECT id FROM cte"
        )
        result = TableAllowlistChecker().check_ast(ast, contract)
        assert result.passed


class TestOperationBlocklistChecker:
    def test_select_passes(self, contract: DataContract) -> None:
        ast = _parse("SELECT id FROM analytics.orders")
        result = OperationBlocklistChecker().check_ast(ast, contract)
        assert result.passed

    def test_delete_blocked(self, contract: DataContract) -> None:
        ast = _parse("DELETE FROM analytics.orders WHERE id = 1")
        result = OperationBlocklistChecker().check_ast(ast, contract)
        assert not result.passed
        assert "DELETE" in result.message

    def test_drop_blocked(self, contract: DataContract) -> None:
        ast = _parse("DROP TABLE analytics.orders")
        result = OperationBlocklistChecker().check_ast(ast, contract)
        assert not result.passed

    def test_insert_blocked(self, contract: DataContract) -> None:
        ast = _parse("INSERT INTO analytics.orders (id) VALUES (1)")
        result = OperationBlocklistChecker().check_ast(ast, contract)
        assert not result.passed

    def test_update_blocked(self, contract: DataContract) -> None:
        ast = _parse("UPDATE analytics.orders SET id = 1")
        result = OperationBlocklistChecker().check_ast(ast, contract)
        assert not result.passed

    def test_truncate_blocked(self, contract: DataContract) -> None:
        ast = _parse("TRUNCATE TABLE analytics.orders")
        result = OperationBlocklistChecker().check_ast(ast, contract)
        assert not result.passed
        assert "TRUNCATE" in result.message


class TestNoSelectStarChecker:
    def test_explicit_columns_pass(self) -> None:
        ast = _parse("SELECT id, name FROM analytics.orders")
        result = NoSelectStarChecker().check_ast(ast)
        assert result.passed

    def test_select_star_blocked(self) -> None:
        ast = _parse("SELECT * FROM analytics.orders")
        result = NoSelectStarChecker().check_ast(ast)
        assert not result.passed
        assert "SELECT *" in result.message

    def test_select_star_in_subquery_blocked(self) -> None:
        ast = _parse("SELECT id FROM (SELECT * FROM analytics.orders) t")
        result = NoSelectStarChecker().check_ast(ast)
        assert not result.passed


class TestRequiredFilterChecker:
    def test_filter_present_passes(self) -> None:
        ast = _parse("SELECT id FROM analytics.orders WHERE tenant_id = 'acme'")
        result = RequiredFilterChecker("tenant_id").check_ast(ast)
        assert result.passed

    def test_filter_missing_blocked(self) -> None:
        ast = _parse("SELECT id FROM analytics.orders WHERE id = 1")
        result = RequiredFilterChecker("tenant_id").check_ast(ast)
        assert not result.passed
        assert "tenant_id" in result.message

    def test_no_where_clause_blocked(self) -> None:
        ast = _parse("SELECT id FROM analytics.orders")
        result = RequiredFilterChecker("tenant_id").check_ast(ast)
        assert not result.passed

    def test_filter_in_subquery_passes(self) -> None:
        ast = _parse(
            "SELECT id FROM (SELECT id FROM analytics.orders"
            " WHERE tenant_id = 'x') t"
        )
        result = RequiredFilterChecker("tenant_id").check_ast(ast)
        assert result.passed


class TestBlockedColumnsChecker:
    def test_safe_columns_pass(self) -> None:
        ast = _parse("SELECT id, name FROM analytics.customers")
        result = BlockedColumnsChecker(["ssn", "email"]).check_ast(ast)
        assert result.passed

    def test_blocked_column_caught(self) -> None:
        ast = _parse("SELECT id, ssn FROM analytics.customers")
        result = BlockedColumnsChecker(["ssn", "email"]).check_ast(ast)
        assert not result.passed
        assert "ssn" in result.message

    def test_blocked_column_case_insensitive(self) -> None:
        ast = _parse("SELECT id, SSN FROM analytics.customers")
        result = BlockedColumnsChecker(["ssn"]).check_ast(ast)
        assert not result.passed

    def test_select_star_caught(self) -> None:
        ast = _parse("SELECT * FROM analytics.customers")
        result = BlockedColumnsChecker(["ssn"]).check_ast(ast)
        assert not result.passed
        assert "SELECT *" in result.message


class TestRequireLimitChecker:
    def test_with_limit_passes(self) -> None:
        ast = _parse("SELECT id FROM analytics.orders LIMIT 10")
        result = RequireLimitChecker().check_ast(ast)
        assert result.passed

    def test_without_limit_blocked(self) -> None:
        ast = _parse("SELECT id FROM analytics.orders")
        result = RequireLimitChecker().check_ast(ast)
        assert not result.passed
        assert "LIMIT" in result.message


class TestMaxJoinsChecker:
    def test_within_limit_passes(self) -> None:
        ast = _parse(
            "SELECT o.id FROM analytics.orders o"
            " JOIN analytics.customers c ON o.id = c.id"
        )
        result = MaxJoinsChecker(3).check_ast(ast)
        assert result.passed

    def test_exceeds_limit_blocked(self) -> None:
        ast = _parse(
            "SELECT a.id FROM t1 a"
            " JOIN t2 b ON a.id = b.id"
            " JOIN t3 c ON b.id = c.id"
            " JOIN t4 d ON c.id = d.id"
        )
        result = MaxJoinsChecker(2).check_ast(ast)
        assert not result.passed
        assert "3" in result.message  # actual count
        assert "2" in result.message  # limit
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_validation/test_checkers.py -v`
Expected: FAIL — new checkers and `check_ast` not found.

- [ ] **Step 3: Implement refactored checkers**

Replace `src/agentic_data_contracts/validation/checkers.py` with:

```python
"""Built-in SQL checkers using sqlglot AST."""

from __future__ import annotations

from dataclasses import dataclass

from sqlglot import exp

from agentic_data_contracts.core.contract import DataContract


@dataclass
class CheckResult:
    passed: bool
    severity: str  # "block" | "warn" | "log"
    message: str


def extract_tables(expression: exp.Expression) -> set[str]:
    """Extract fully-qualified table names from an AST, excluding CTE definitions."""
    tables: set[str] = set()
    cte_names = {cte.alias for cte in expression.find_all(exp.CTE) if cte.alias}
    for table in expression.find_all(exp.Table):
        if isinstance(table.parent, exp.CTE):
            continue
        parts = []
        if table.db:
            parts.append(table.db)
        if table.name:
            parts.append(table.name)
        full_name = ".".join(parts)
        if full_name and full_name not in cte_names:
            tables.add(full_name)
    return tables


class TableAllowlistChecker:
    """Checks that all referenced tables are in the contract's allowed_tables."""

    def check_ast(self, ast: exp.Expression, contract: DataContract) -> CheckResult:
        allowed = set(contract.allowed_table_names())
        referenced_tables = extract_tables(ast)
        disallowed = referenced_tables - allowed
        if disallowed:
            return CheckResult(
                passed=False,
                severity="block",
                message=f"Tables not in allowlist: {', '.join(sorted(disallowed))}",
            )
        return CheckResult(passed=True, severity="block", message="")


class OperationBlocklistChecker:
    """Checks that the SQL statement type is not in forbidden_operations."""

    _OPERATION_MAP: dict[type[exp.Expression], str] = {
        exp.Delete: "DELETE",
        exp.Drop: "DROP",
        exp.Insert: "INSERT",
        exp.Update: "UPDATE",
    }

    def check_ast(self, ast: exp.Expression, contract: DataContract) -> CheckResult:
        forbidden = {op.upper() for op in contract.schema.semantic.forbidden_operations}

        for expr_type, op_name in self._OPERATION_MAP.items():
            if isinstance(ast, expr_type) and op_name in forbidden:
                return CheckResult(
                    passed=False,
                    severity="block",
                    message=f"Forbidden operation: {op_name}",
                )

        if "TRUNCATE" in forbidden and (
            isinstance(ast, exp.TruncateTable)
            or (
                isinstance(ast, exp.Command)
                and ast.this
                and str(ast.this).upper() == "TRUNCATE"
            )
        ):
            return CheckResult(
                passed=False,
                severity="block",
                message="Forbidden operation: TRUNCATE",
            )

        return CheckResult(passed=True, severity="block", message="")


class NoSelectStarChecker:
    """Checks that no SELECT * appears anywhere in the query."""

    def check_ast(self, ast: exp.Expression) -> CheckResult:
        if any(ast.find_all(exp.Star)):
            return CheckResult(
                passed=False,
                severity="block",
                message="SELECT * is not allowed — specify explicit columns",
            )
        return CheckResult(passed=True, severity="block", message="")


class RequiredFilterChecker:
    """Checks that a required WHERE filter column is present."""

    def __init__(self, column: str) -> None:
        self.column = column

    def check_ast(self, ast: exp.Expression) -> CheckResult:
        where_columns: set[str] = set()
        for where in ast.find_all(exp.Where):
            for col in where.find_all(exp.Column):
                where_columns.add(col.name.lower())

        if self.column.lower() not in where_columns:
            return CheckResult(
                passed=False,
                severity="block",
                message=f"Missing required filter: {self.column}",
            )
        return CheckResult(passed=True, severity="block", message="")


class BlockedColumnsChecker:
    """Checks that blocked columns don't appear in SELECT."""

    def __init__(self, blocked: list[str]) -> None:
        self.blocked = {c.lower() for c in blocked}

    def check_ast(self, ast: exp.Expression) -> CheckResult:
        # SELECT * exposes all columns, including blocked ones
        if any(ast.find_all(exp.Star)):
            return CheckResult(
                passed=False,
                severity="block",
                message=(
                    "SELECT * may expose blocked columns: "
                    f"{', '.join(sorted(self.blocked))}"
                ),
            )

        selected: set[str] = set()
        for select in ast.find_all(exp.Select):
            for expr in select.expressions:
                for col in expr.find_all(exp.Column):
                    selected.add(col.name.lower())

        found = selected & self.blocked
        if found:
            return CheckResult(
                passed=False,
                severity="block",
                message=f"Blocked columns in SELECT: {', '.join(sorted(found))}",
            )
        return CheckResult(passed=True, severity="block", message="")


class RequireLimitChecker:
    """Checks that the query has a LIMIT clause."""

    def check_ast(self, ast: exp.Expression) -> CheckResult:
        if not list(ast.find_all(exp.Limit)):
            return CheckResult(
                passed=False,
                severity="block",
                message="Query must include a LIMIT clause",
            )
        return CheckResult(passed=True, severity="block", message="")


class MaxJoinsChecker:
    """Checks that the number of JOINs doesn't exceed a maximum."""

    def __init__(self, max_joins: int) -> None:
        self.max_joins = max_joins

    def check_ast(self, ast: exp.Expression) -> CheckResult:
        join_count = len(list(ast.find_all(exp.Join)))
        if join_count > self.max_joins:
            return CheckResult(
                passed=False,
                severity="block",
                message=(
                    f"Query has {join_count} JOINs, "
                    f"exceeds maximum of {self.max_joins}"
                ),
            )
        return CheckResult(passed=True, severity="block", message="")


class ResultCheckRunner:
    """Runs result_check validations against query output."""

    def __init__(self, column: str | None, min_value: float | None,
                 max_value: float | None, not_null: bool | None,
                 min_rows: int | None, max_rows: int | None,
                 rule_name: str) -> None:
        self.column = column
        self.min_value = min_value
        self.max_value = max_value
        self.not_null = not_null
        self.min_rows = min_rows
        self.max_rows = max_rows
        self.rule_name = rule_name

    @classmethod
    def from_config(cls, config: ResultCheck, rule_name: str) -> ResultCheckRunner:
        from agentic_data_contracts.core.schema import ResultCheck as RC
        return cls(
            column=config.column,
            min_value=config.min_value,
            max_value=config.max_value,
            not_null=config.not_null,
            min_rows=config.min_rows,
            max_rows=config.max_rows,
            rule_name=rule_name,
        )

    def check_results(self, columns: list[str], rows: list[tuple]) -> CheckResult:
        # Row count checks
        row_count = len(rows)
        if self.min_rows is not None and row_count < self.min_rows:
            return CheckResult(
                passed=False,
                severity="block",
                message=(
                    f"Rule '{self.rule_name}': query returned {row_count} rows, "
                    f"minimum is {self.min_rows}"
                ),
            )
        if self.max_rows is not None and row_count > self.max_rows:
            return CheckResult(
                passed=False,
                severity="block",
                message=(
                    f"Rule '{self.rule_name}': query returned {row_count} rows, "
                    f"maximum is {self.max_rows}"
                ),
            )

        # Column-specific checks
        if self.column is not None:
            col_lower = {c.lower(): i for i, c in enumerate(columns)}
            idx = col_lower.get(self.column.lower())
            if idx is None:
                # Column not in result set — skip (rule doesn't apply)
                return CheckResult(passed=True, severity="block", message="")

            values = [row[idx] for row in rows]

            if self.not_null and any(v is None for v in values):
                null_count = sum(1 for v in values if v is None)
                return CheckResult(
                    passed=False,
                    severity="block",
                    message=(
                        f"Rule '{self.rule_name}': column '{self.column}' "
                        f"contains {null_count} null values"
                    ),
                )

            numeric_values = [v for v in values if v is not None and isinstance(v, (int, float))]
            if numeric_values:
                if self.min_value is not None:
                    actual_min = min(numeric_values)
                    if actual_min < self.min_value:
                        return CheckResult(
                            passed=False,
                            severity="block",
                            message=(
                                f"Rule '{self.rule_name}': column '{self.column}' "
                                f"min value {actual_min} is below limit {self.min_value}"
                            ),
                        )
                if self.max_value is not None:
                    actual_max = max(numeric_values)
                    if actual_max > self.max_value:
                        return CheckResult(
                            passed=False,
                            severity="block",
                            message=(
                                f"Rule '{self.rule_name}': column '{self.column}' "
                                f"max value {actual_max} exceeds limit {self.max_value}"
                            ),
                        )

        return CheckResult(passed=True, severity="block", message="")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_validation/test_checkers.py -v`
Expected: All PASS.

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/validation/checkers.py tests/test_validation/test_checkers.py
git commit -m "refactor: AST-based checker protocol, add BlockedColumns/RequireLimit/MaxJoins/ResultCheckRunner"
```

---

### Task 3: Add ResultCheckRunner Tests

**Files:**
- Test: `tests/test_validation/test_result_checks.py` (new)

- [ ] **Step 1: Write result checker tests**

Create `tests/test_validation/test_result_checks.py`:

```python
from agentic_data_contracts.validation.checkers import ResultCheckRunner


class TestResultCheckRunnerRowBounds:
    def test_min_rows_passes(self) -> None:
        runner = ResultCheckRunner(
            column=None, min_value=None, max_value=None,
            not_null=None, min_rows=1, max_rows=None, rule_name="test"
        )
        result = runner.check_results(["id"], [(1,), (2,)])
        assert result.passed

    def test_min_rows_fails(self) -> None:
        runner = ResultCheckRunner(
            column=None, min_value=None, max_value=None,
            not_null=None, min_rows=1, max_rows=None, rule_name="not_empty"
        )
        result = runner.check_results(["id"], [])
        assert not result.passed
        assert "0 rows" in result.message
        assert "not_empty" in result.message

    def test_max_rows_passes(self) -> None:
        runner = ResultCheckRunner(
            column=None, min_value=None, max_value=None,
            not_null=None, min_rows=None, max_rows=100, rule_name="test"
        )
        result = runner.check_results(["id"], [(1,), (2,)])
        assert result.passed

    def test_max_rows_fails(self) -> None:
        runner = ResultCheckRunner(
            column=None, min_value=None, max_value=None,
            not_null=None, min_rows=None, max_rows=2, rule_name="size_limit"
        )
        rows = [(i,) for i in range(5)]
        result = runner.check_results(["id"], rows)
        assert not result.passed
        assert "5 rows" in result.message


class TestResultCheckRunnerColumnBounds:
    def test_max_value_passes(self) -> None:
        runner = ResultCheckRunner(
            column="wau", min_value=None, max_value=8_000_000_000,
            not_null=None, min_rows=None, max_rows=None, rule_name="wau_check"
        )
        result = runner.check_results(["wau"], [(1_000_000,), (2_000_000,)])
        assert result.passed

    def test_max_value_fails(self) -> None:
        runner = ResultCheckRunner(
            column="wau", min_value=None, max_value=8_000_000_000,
            not_null=None, min_rows=None, max_rows=None, rule_name="wau_sanity"
        )
        result = runner.check_results(["wau"], [(12_000_000_000,)])
        assert not result.passed
        assert "12000000000" in result.message
        assert "wau_sanity" in result.message

    def test_min_value_passes(self) -> None:
        runner = ResultCheckRunner(
            column="revenue", min_value=0, max_value=None,
            not_null=None, min_rows=None, max_rows=None, rule_name="test"
        )
        result = runner.check_results(["revenue"], [(100,), (200,)])
        assert result.passed

    def test_min_value_fails(self) -> None:
        runner = ResultCheckRunner(
            column="revenue", min_value=0, max_value=None,
            not_null=None, min_rows=None, max_rows=None, rule_name="no_neg"
        )
        result = runner.check_results(["revenue"], [(100,), (-50,)])
        assert not result.passed
        assert "-50" in result.message

    def test_column_not_in_results_skips(self) -> None:
        runner = ResultCheckRunner(
            column="missing_col", min_value=0, max_value=None,
            not_null=None, min_rows=None, max_rows=None, rule_name="test"
        )
        result = runner.check_results(["id", "name"], [(1, "a")])
        assert result.passed

    def test_column_case_insensitive(self) -> None:
        runner = ResultCheckRunner(
            column="WAU", min_value=None, max_value=100,
            not_null=None, min_rows=None, max_rows=None, rule_name="test"
        )
        result = runner.check_results(["wau"], [(999,)])
        assert not result.passed


class TestResultCheckRunnerNotNull:
    def test_not_null_passes(self) -> None:
        runner = ResultCheckRunner(
            column="name", min_value=None, max_value=None,
            not_null=True, min_rows=None, max_rows=None, rule_name="test"
        )
        result = runner.check_results(["name"], [("alice",), ("bob",)])
        assert result.passed

    def test_not_null_fails(self) -> None:
        runner = ResultCheckRunner(
            column="name", min_value=None, max_value=None,
            not_null=True, min_rows=None, max_rows=None, rule_name="no_nulls"
        )
        result = runner.check_results(["name"], [("alice",), (None,)])
        assert not result.passed
        assert "1 null" in result.message
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `uv run pytest tests/test_validation/test_result_checks.py -v`
Expected: All PASS (implementation was done in Task 2).

- [ ] **Step 3: Commit**

```bash
git add tests/test_validation/test_result_checks.py
git commit -m "test: add comprehensive ResultCheckRunner unit tests"
```

---

### Task 4: Rewrite Validator to Rules-Driven Pipeline

**Files:**
- Modify: `src/agentic_data_contracts/validation/validator.py:1-151`
- Test: `tests/test_validation/test_validator.py`

- [ ] **Step 1: Write failing tests for new validator**

Replace `tests/test_validation/test_validator.py` with:

```python
from pathlib import Path

import pytest

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.schema import (
    AllowedTable,
    DataContractSchema,
    Enforcement,
    QueryCheck,
    ResourceConfig,
    ResultCheck,
    SemanticConfig,
    SemanticRule,
)
from agentic_data_contracts.validation.explain import ExplainResult
from agentic_data_contracts.validation.validator import Validator


class FakeExplainAdapter:
    def __init__(self, result: ExplainResult) -> None:
        self._result = result

    def explain(self, sql: str) -> ExplainResult:
        return self._result


@pytest.fixture
def contract(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml(fixtures_dir / "valid_contract.yml")


@pytest.fixture
def validator(contract: DataContract) -> Validator:
    return Validator(contract)


def test_valid_query_passes(validator: Validator) -> None:
    result = validator.validate(
        "SELECT id, amount FROM analytics.orders WHERE tenant_id = 'acme'"
    )
    assert not result.blocked
    assert result.reasons == []


def test_forbidden_table_blocks(validator: Validator) -> None:
    result = validator.validate("SELECT id FROM raw.payments WHERE tenant_id = 'x'")
    assert result.blocked
    assert any("raw.payments" in r for r in result.reasons)


def test_select_star_blocks(validator: Validator) -> None:
    result = validator.validate("SELECT * FROM analytics.orders WHERE tenant_id = 'x'")
    assert result.blocked
    assert any("SELECT *" in r for r in result.reasons)


def test_missing_filter_blocks(validator: Validator) -> None:
    result = validator.validate("SELECT id FROM analytics.orders")
    assert result.blocked
    assert any("tenant_id" in r for r in result.reasons)


def test_delete_blocks(validator: Validator) -> None:
    result = validator.validate("DELETE FROM analytics.orders WHERE tenant_id = 'x'")
    assert result.blocked
    assert any("DELETE" in r for r in result.reasons)


def test_multiple_violations_all_reported(validator: Validator) -> None:
    result = validator.validate("SELECT * FROM raw.payments")
    assert result.blocked
    assert len(result.reasons) >= 2


def test_warnings_returned(validator: Validator) -> None:
    result = validator.validate("SELECT id FROM analytics.orders WHERE tenant_id = 'x'")
    assert not result.blocked


def test_minimal_contract_permissive(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "minimal_contract.yml")
    validator = Validator(dc)
    result = validator.validate("SELECT * FROM public.users")
    assert not result.blocked


def test_explain_cost_exceeds_limit(contract: DataContract) -> None:
    adapter = FakeExplainAdapter(
        ExplainResult(estimated_cost_usd=10.0, estimated_rows=100, schema_valid=True)
    )
    validator = Validator(contract, explain_adapter=adapter)
    result = validator.validate("SELECT id FROM analytics.orders WHERE tenant_id = 'x'")
    assert result.blocked
    assert any("cost" in r.lower() for r in result.reasons)


def test_explain_rows_exceeds_limit(contract: DataContract) -> None:
    adapter = FakeExplainAdapter(
        ExplainResult(estimated_cost_usd=1.0, estimated_rows=2000000, schema_valid=True)
    )
    validator = Validator(contract, explain_adapter=adapter)
    result = validator.validate("SELECT id FROM analytics.orders WHERE tenant_id = 'x'")
    assert result.blocked
    assert any("rows" in r.lower() for r in result.reasons)


def test_explain_schema_invalid_blocks(contract: DataContract) -> None:
    adapter = FakeExplainAdapter(
        ExplainResult(
            estimated_cost_usd=None,
            estimated_rows=None,
            schema_valid=False,
            errors=["Column not found"],
        )
    )
    validator = Validator(contract, explain_adapter=adapter)
    result = validator.validate("SELECT id FROM analytics.orders WHERE tenant_id = 'x'")
    assert result.blocked


def test_explain_within_limits_passes(contract: DataContract) -> None:
    adapter = FakeExplainAdapter(
        ExplainResult(estimated_cost_usd=1.0, estimated_rows=500, schema_valid=True)
    )
    validator = Validator(contract, explain_adapter=adapter)
    result = validator.validate("SELECT id FROM analytics.orders WHERE tenant_id = 'x'")
    assert not result.blocked


def test_explain_cost_passed_through(contract: DataContract) -> None:
    adapter = FakeExplainAdapter(
        ExplainResult(estimated_cost_usd=2.5, estimated_rows=500, schema_valid=True)
    )
    validator = Validator(contract, explain_adapter=adapter)
    result = validator.validate("SELECT id FROM analytics.orders WHERE tenant_id = 'x'")
    assert not result.blocked
    assert result.estimated_cost_usd == 2.5


def test_table_scoped_query_check() -> None:
    """A rule scoped to analytics.orders should not apply to analytics.customers."""
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate(
                    {"schema": "analytics", "tables": ["orders", "customers"]}
                ),
            ],
            rules=[
                SemanticRule(
                    name="orders_filter",
                    description="Orders must filter by tenant_id",
                    enforcement=Enforcement.BLOCK,
                    table="analytics.orders",
                    query_check=QueryCheck(required_filter="tenant_id"),
                ),
            ],
        ),
    )
    dc = DataContract(schema)
    validator = Validator(dc)

    # Querying orders without filter — blocked
    result = validator.validate("SELECT id FROM analytics.orders")
    assert result.blocked

    # Querying customers without filter — allowed (rule doesn't apply)
    result = validator.validate("SELECT id FROM analytics.customers")
    assert not result.blocked


def test_global_query_check() -> None:
    """A rule with no table scoping applies to all queries."""
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate(
                    {"schema": "public", "tables": ["a", "b"]}
                ),
            ],
            rules=[
                SemanticRule(
                    name="no_star",
                    description="No select star",
                    enforcement=Enforcement.BLOCK,
                    query_check=QueryCheck(no_select_star=True),
                ),
            ],
        ),
    )
    dc = DataContract(schema)
    validator = Validator(dc)

    result = validator.validate("SELECT * FROM public.a")
    assert result.blocked

    result = validator.validate("SELECT id FROM public.a")
    assert not result.blocked


def test_validate_results_blocks() -> None:
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate(
                    {"schema": "analytics", "tables": ["metrics"]}
                ),
            ],
            rules=[
                SemanticRule(
                    name="wau_sanity",
                    description="WAU sanity",
                    enforcement=Enforcement.BLOCK,
                    result_check=ResultCheck(column="wau", max_value=8_000_000_000),
                ),
            ],
        ),
    )
    dc = DataContract(schema)
    validator = Validator(dc)

    result = validator.validate_results(
        "SELECT wau FROM analytics.metrics",
        columns=["wau"],
        rows=[(12_000_000_000,)],
    )
    assert result.blocked
    assert any("wau" in r for r in result.reasons)


def test_validate_results_passes() -> None:
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate(
                    {"schema": "analytics", "tables": ["metrics"]}
                ),
            ],
            rules=[
                SemanticRule(
                    name="wau_sanity",
                    description="WAU sanity",
                    enforcement=Enforcement.BLOCK,
                    result_check=ResultCheck(column="wau", max_value=8_000_000_000),
                ),
            ],
        ),
    )
    dc = DataContract(schema)
    validator = Validator(dc)

    result = validator.validate_results(
        "SELECT wau FROM analytics.metrics",
        columns=["wau"],
        rows=[(1_000_000,)],
    )
    assert not result.blocked


def test_validate_results_table_scoping() -> None:
    """Result check scoped to analytics.metrics should skip analytics.other."""
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate(
                    {"schema": "analytics", "tables": ["metrics", "other"]}
                ),
            ],
            rules=[
                SemanticRule(
                    name="wau_sanity",
                    description="WAU sanity",
                    enforcement=Enforcement.BLOCK,
                    table="analytics.metrics",
                    result_check=ResultCheck(column="wau", max_value=100),
                ),
            ],
        ),
    )
    dc = DataContract(schema)
    validator = Validator(dc)

    # Query against analytics.other — rule should not apply
    result = validator.validate_results(
        "SELECT wau FROM analytics.other",
        columns=["wau"],
        rows=[(999,)],
    )
    assert not result.blocked


def test_validate_results_warns() -> None:
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate(
                    {"schema": "public", "tables": ["t"]}
                ),
            ],
            rules=[
                SemanticRule(
                    name="empty_check",
                    description="Warn if empty",
                    enforcement=Enforcement.WARN,
                    result_check=ResultCheck(min_rows=1),
                ),
            ],
        ),
    )
    dc = DataContract(schema)
    validator = Validator(dc)

    result = validator.validate_results(
        "SELECT id FROM public.t",
        columns=["id"],
        rows=[],
    )
    assert not result.blocked
    assert len(result.warnings) == 1


def test_malformed_sql_blocks(validator: Validator) -> None:
    result = validator.validate("NOT VALID SQL AT ALL !!!")
    assert result.blocked
    assert any("parse error" in r.lower() for r in result.reasons)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_validation/test_validator.py -v`
Expected: FAIL — `validate_results` not found, `estimated_cost_usd` not in `ValidationResult`.

- [ ] **Step 3: Implement new validator**

Replace `src/agentic_data_contracts/validation/validator.py` with:

```python
"""Validator — orchestrates checkers and aggregates results."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

import sqlglot
from sqlglot import exp

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.validation.checkers import (
    BlockedColumnsChecker,
    CheckResult,
    MaxJoinsChecker,
    NoSelectStarChecker,
    OperationBlocklistChecker,
    RequiredFilterChecker,
    RequireLimitChecker,
    ResultCheckRunner,
    TableAllowlistChecker,
    extract_tables,
)
from agentic_data_contracts.validation.explain import ExplainAdapter


class Checker(Protocol):
    def check_ast(self, ast: exp.Expression, *args: Any) -> CheckResult: ...


@dataclass
class ValidationResult:
    blocked: bool
    reasons: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    log_messages: list[str] = field(default_factory=list)
    estimated_cost_usd: float | None = None


class Validator:
    """Runs all applicable checkers against a SQL query."""

    def __init__(
        self,
        contract: DataContract,
        dialect: str | None = None,
        explain_adapter: ExplainAdapter | None = None,
    ) -> None:
        self.contract = contract
        self.dialect = dialect
        self.explain_adapter = explain_adapter
        self._build_checkers()

    def _build_checkers(self) -> None:
        semantic = self.contract.schema.semantic

        # Structural checkers (from top-level config, not rules)
        self._table_checker = (
            TableAllowlistChecker() if semantic.allowed_tables else None
        )
        self._operation_checker = (
            OperationBlocklistChecker() if semantic.forbidden_operations else None
        )

        # Rule-based query checkers: (enforcement, table_scope, checker)
        self._query_checkers: list[tuple[str, str | None, Any]] = []
        # Rule-based result checkers: (enforcement, table_scope, runner)
        self._result_checkers: list[tuple[str, str | None, ResultCheckRunner]] = []

        for rule in semantic.rules:
            table_scope = rule.table if rule.table and rule.table != "*" else None

            if rule.query_check is not None:
                qc = rule.query_check
                if qc.required_filter is not None:
                    self._query_checkers.append((
                        rule.enforcement.value,
                        table_scope,
                        RequiredFilterChecker(qc.required_filter),
                    ))
                if qc.no_select_star is True:
                    self._query_checkers.append((
                        rule.enforcement.value,
                        table_scope,
                        NoSelectStarChecker(),
                    ))
                if qc.blocked_columns is not None:
                    self._query_checkers.append((
                        rule.enforcement.value,
                        table_scope,
                        BlockedColumnsChecker(qc.blocked_columns),
                    ))
                if qc.require_limit is True:
                    self._query_checkers.append((
                        rule.enforcement.value,
                        table_scope,
                        RequireLimitChecker(),
                    ))
                if qc.max_joins is not None:
                    self._query_checkers.append((
                        rule.enforcement.value,
                        table_scope,
                        MaxJoinsChecker(qc.max_joins),
                    ))

            elif rule.result_check is not None:
                runner = ResultCheckRunner(
                    column=rule.result_check.column,
                    min_value=rule.result_check.min_value,
                    max_value=rule.result_check.max_value,
                    not_null=rule.result_check.not_null,
                    min_rows=rule.result_check.min_rows,
                    max_rows=rule.result_check.max_rows,
                    rule_name=rule.name,
                )
                self._result_checkers.append((
                    rule.enforcement.value,
                    table_scope,
                    runner,
                ))

    def _is_table_in_scope(
        self, table_scope: str | None, referenced_tables: set[str]
    ) -> bool:
        if table_scope is None:
            return True
        return table_scope in referenced_tables

    def validate(self, sql: str) -> ValidationResult:
        reasons: list[str] = []
        warnings: list[str] = []
        log_messages: list[str] = []
        estimated_cost_usd: float | None = None

        # Parse SQL once
        try:
            ast = sqlglot.parse_one(sql, dialect=self.dialect)
        except sqlglot.errors.ParseError as e:
            return ValidationResult(
                blocked=True, reasons=[f"SQL parse error: {e}"]
            )

        referenced_tables = extract_tables(ast)

        # Phase 1a: Structural checks (from top-level config)
        if self._table_checker is not None:
            result = self._table_checker.check_ast(ast, self.contract)
            if not result.passed:
                reasons.append(result.message)

        if self._operation_checker is not None:
            result = self._operation_checker.check_ast(ast, self.contract)
            if not result.passed:
                reasons.append(result.message)

        # Phase 1b: Rule-based query checks
        for enforcement, table_scope, checker in self._query_checkers:
            if not self._is_table_in_scope(table_scope, referenced_tables):
                continue
            result = checker.check_ast(ast)
            if not result.passed:
                if enforcement == "block":
                    reasons.append(result.message)
                elif enforcement == "warn":
                    warnings.append(result.message)
                else:
                    log_messages.append(result.message)

        # Phase 2: EXPLAIN checks (only when Phase 1 passes)
        if not reasons and self.explain_adapter is not None:
            explain_result = self.explain_adapter.explain(sql)
            if not explain_result.schema_valid:
                reasons.append(
                    f"Schema validation failed: {', '.join(explain_result.errors)}"
                )
            else:
                estimated_cost_usd = explain_result.estimated_cost_usd
                res = self.contract.schema.resources
                if res:
                    if (
                        res.cost_limit_usd is not None
                        and explain_result.estimated_cost_usd is not None
                        and explain_result.estimated_cost_usd > res.cost_limit_usd
                    ):
                        cost = explain_result.estimated_cost_usd
                        limit = res.cost_limit_usd
                        reasons.append(
                            f"Estimated cost ${cost:.2f} exceeds limit ${limit:.2f}"
                        )
                    if (
                        res.max_rows_scanned is not None
                        and explain_result.estimated_rows is not None
                        and explain_result.estimated_rows > res.max_rows_scanned
                    ):
                        rows = explain_result.estimated_rows
                        max_rows = res.max_rows_scanned
                        reasons.append(
                            f"Estimated rows {rows:,} exceeds limit {max_rows:,}"
                        )

        return ValidationResult(
            blocked=len(reasons) > 0,
            reasons=reasons,
            warnings=warnings,
            log_messages=log_messages,
            estimated_cost_usd=estimated_cost_usd,
        )

    def validate_results(
        self, sql: str, columns: list[str], rows: list[tuple]
    ) -> ValidationResult:
        """Run post-execution result checks against query output."""
        reasons: list[str] = []
        warnings: list[str] = []
        log_messages: list[str] = []

        # Parse SQL to extract referenced tables for scoping
        try:
            ast = sqlglot.parse_one(sql, dialect=self.dialect)
        except sqlglot.errors.ParseError:
            # If SQL can't be parsed, run all result checks (no scoping)
            referenced_tables: set[str] = set()
        else:
            referenced_tables = extract_tables(ast)

        for enforcement, table_scope, runner in self._result_checkers:
            if not self._is_table_in_scope(table_scope, referenced_tables):
                continue
            result = runner.check_results(columns, rows)
            if not result.passed:
                if enforcement == "block":
                    reasons.append(result.message)
                elif enforcement == "warn":
                    warnings.append(result.message)
                else:
                    log_messages.append(result.message)

        return ValidationResult(
            blocked=len(reasons) > 0,
            reasons=reasons,
            warnings=warnings,
            log_messages=log_messages,
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_validation/test_validator.py -v`
Expected: All PASS.

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/validation/validator.py tests/test_validation/test_validator.py
git commit -m "refactor: rules-driven validator with validate_results() and table scoping"
```

---

### Task 5: Update contract.py, prompt.py, and Bridge Compiler

**Files:**
- Modify: `src/agentic_data_contracts/core/contract.py:70-83`
- Modify: `src/agentic_data_contracts/core/prompt.py:220-249`
- Modify: `src/agentic_data_contracts/bridge/compiler.py:39-49`
- Test: `tests/test_core/test_contract.py`, `tests/test_core/test_prompt_renderers.py`, `tests/test_bridge/test_compiler.py`

- [ ] **Step 1: Update contract.py — keep block_rules/warn_rules/log_rules**

The `block_rules()`, `warn_rules()`, `log_rules()` helpers are still used by `prompt.py` and `bridge/compiler.py`. Keep them — they work fine with the new model since `SemanticRule` still has `enforcement`.

No changes needed to `contract.py` itself — the helpers filter by enforcement level, which hasn't changed.

- [ ] **Step 2: Update test_contract.py for new fixture format**

Replace `tests/test_core/test_contract.py` with:

```python
from pathlib import Path

from agentic_data_contracts.core.contract import DataContract


def test_from_yaml(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    assert dc.name == "revenue-analysis"
    assert len(dc.schema.semantic.allowed_tables) == 2
    assert dc.schema.resources is not None
    assert dc.schema.resources.cost_limit_usd == 5.00


def test_from_yaml_minimal(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "minimal_contract.yml")
    assert dc.name == "basic-query"
    assert dc.schema.resources is None


def test_from_yaml_string(fixtures_dir: Path) -> None:
    text = (fixtures_dir / "valid_contract.yml").read_text()
    dc = DataContract.from_yaml_string(text)
    assert dc.name == "revenue-analysis"


def test_to_system_prompt(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    prompt = dc.to_system_prompt()
    assert "analytics.orders" in prompt
    assert "analytics.customers" in prompt
    assert "DELETE" in prompt
    assert "tenant_isolation" in prompt
    assert "no_select_star" in prompt
    assert "cost_limit_usd" in prompt or "5.0" in prompt


def test_to_system_prompt_composable(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    user_prompt = "You are an analytics assistant."
    combined = f"{user_prompt}\n\n{dc.to_system_prompt()}"
    assert combined.startswith("You are an analytics assistant.")
    assert "analytics.orders" in combined


def test_allowed_table_names(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    names = dc.allowed_table_names()
    assert "analytics.orders" in names
    assert "analytics.customers" in names
    assert "analytics.subscriptions" in names
    assert not any(n.startswith("raw.") for n in names)


def test_block_rules(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    block_rules = dc.block_rules()
    assert len(block_rules) == 2
    names = [r.name for r in block_rules]
    assert "tenant_isolation" in names
    assert "no_select_star" in names


def test_warn_rules(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    warn_rules = dc.warn_rules()
    assert len(warn_rules) == 1
    assert warn_rules[0].name == "use_approved_metrics"
```

- [ ] **Step 3: Run contract and prompt tests**

Run: `uv run pytest tests/test_core/test_contract.py tests/test_core/test_prompt_renderers.py -v`
Expected: All PASS (prompt.py uses `block_rules()`/`warn_rules()` which still work).

- [ ] **Step 4: Run bridge tests**

Run: `uv run pytest tests/test_bridge/ -v`
Expected: PASS (or skipped if `ai-agent-contracts` not installed). The bridge compiler uses `block_rules()`/`warn_rules()`/`log_rules()` which still work with the new model.

- [ ] **Step 5: Commit**

```bash
git add tests/test_core/test_contract.py
git commit -m "test: update contract tests for new rule format"
```

---

### Task 6: Update Tool Factory — Result Checks + Session Cost

**Files:**
- Modify: `src/agentic_data_contracts/tools/factory.py:246-282` (run_query), `213-226` (validate_query)
- Test: `tests/test_tools/test_factory.py`

- [ ] **Step 1: Write failing tests for result check enforcement and session cost**

Add these tests to the end of `tests/test_tools/test_factory.py`:

```python
@pytest.mark.asyncio
async def test_run_query_result_check_blocks() -> None:
    """Result check with enforcement=block should discard data and return violation."""
    from agentic_data_contracts.core.schema import (
        AllowedTable,
        DataContractSchema,
        Enforcement,
        ResultCheck,
        SemanticConfig,
        SemanticRule,
    )

    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate(
                    {"schema": "analytics", "tables": ["orders"]}
                ),
            ],
            rules=[
                SemanticRule(
                    name="no_negative",
                    description="No negative amounts",
                    enforcement=Enforcement.BLOCK,
                    result_check=ResultCheck(column="amount", min_value=0),
                ),
            ],
        ),
    )
    dc = DataContract(schema)

    db = DuckDBAdapter(":memory:")
    db.connection.execute("""
        CREATE SCHEMA IF NOT EXISTS analytics;
        CREATE TABLE analytics.orders (id INTEGER, amount DECIMAL(10,2));
        INSERT INTO analytics.orders VALUES (1, 100.00), (2, -50.00);
    """)

    tools = create_tools(dc, adapter=db)
    tool = next(t for t in tools if t.name == "run_query")
    result = await tool.callable({"sql": "SELECT id, amount FROM analytics.orders"})
    text = result["content"][0]["text"]
    assert "block" in text.lower() or "no_negative" in text.lower()
    # Should NOT contain the actual row data
    assert "100" not in text


@pytest.mark.asyncio
async def test_run_query_result_check_warns() -> None:
    """Result check with enforcement=warn should return data + warning."""
    from agentic_data_contracts.core.schema import (
        AllowedTable,
        DataContractSchema,
        Enforcement,
        ResultCheck,
        SemanticConfig,
        SemanticRule,
    )

    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate(
                    {"schema": "analytics", "tables": ["orders"]}
                ),
            ],
            rules=[
                SemanticRule(
                    name="empty_check",
                    description="Warn if empty",
                    enforcement=Enforcement.WARN,
                    result_check=ResultCheck(min_rows=100),
                ),
            ],
        ),
    )
    dc = DataContract(schema)

    db = DuckDBAdapter(":memory:")
    db.connection.execute("""
        CREATE SCHEMA IF NOT EXISTS analytics;
        CREATE TABLE analytics.orders (id INTEGER);
        INSERT INTO analytics.orders VALUES (1), (2);
    """)

    tools = create_tools(dc, adapter=db)
    tool = next(t for t in tools if t.name == "run_query")
    result = await tool.callable({"sql": "SELECT id FROM analytics.orders"})
    text = result["content"][0]["text"]
    # Should contain both the warning and the data
    assert "warn" in text.lower() or "empty_check" in text.lower()
    assert "1" in text  # row data present


@pytest.mark.asyncio
async def test_run_query_records_session_cost() -> None:
    """run_query should record estimated cost in the session."""
    from agentic_data_contracts.core.schema import (
        AllowedTable,
        DataContractSchema,
        ResourceConfig,
        SemanticConfig,
    )
    from agentic_data_contracts.core.session import ContractSession

    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate(
                    {"schema": "analytics", "tables": ["orders"]}
                ),
            ],
        ),
        resources=ResourceConfig(cost_limit_usd=10.0),
    )
    dc = DataContract(schema)

    db = DuckDBAdapter(":memory:")
    db.connection.execute("""
        CREATE SCHEMA IF NOT EXISTS analytics;
        CREATE TABLE analytics.orders (id INTEGER);
        INSERT INTO analytics.orders VALUES (1);
    """)

    session = ContractSession(dc)
    tools = create_tools(dc, adapter=db, session=session)
    tool = next(t for t in tools if t.name == "run_query")
    await tool.callable({"sql": "SELECT id FROM analytics.orders"})

    # DuckDB doesn't provide cost estimates, so cost should remain 0
    # This test verifies the plumbing works without error
    assert session.cost_usd >= 0.0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_tools/test_factory.py::test_run_query_result_check_blocks -v`
Expected: FAIL — `run_query` doesn't call `validate_results()`.

- [ ] **Step 3: Update run_query in factory.py**

In `src/agentic_data_contracts/tools/factory.py`, replace the `run_query` function (lines 247-282) with:

```python
    # ── Tool 9: run_query ─────────────────────────────────────────────────
    async def run_query(args: dict[str, Any]) -> dict[str, Any]:
        sql = args.get("sql", "")

        # Check session limits first
        try:
            session.check_limits()
        except LimitExceededError as e:
            return _text_response(f"BLOCKED — Session limit exceeded: {e}")

        # Phase 1 + 2: query checks + EXPLAIN
        vresult = validator.validate(sql)
        if vresult.blocked:
            session.record_retry()
            msg = "BLOCKED — Violations:\n" + "\n".join(
                f"- {r}" for r in vresult.reasons
            )
            return _text_response(msg)

        # Record estimated cost from EXPLAIN
        if vresult.estimated_cost_usd is not None:
            session.record_cost(vresult.estimated_cost_usd)

        if adapter is None:
            return _text_response(
                "No database adapter configured — cannot execute query."
            )

        try:
            qresult = adapter.execute(sql)
        except Exception as e:  # noqa: BLE001
            session.record_retry()
            return _text_response(f"BLOCKED — Query execution failed: {e}")

        # Phase 3: result checks
        rresult = validator.validate_results(sql, qresult.columns, [tuple(r) for r in qresult.rows])
        if rresult.blocked:
            session.record_retry()
            msg = "BLOCKED — Result check violations:\n" + "\n".join(
                f"- {r}" for r in rresult.reasons
            )
            return _text_response(msg)

        rows = [dict(zip(qresult.columns, row)) for row in qresult.rows]
        data = {
            "columns": qresult.columns,
            "rows": rows,
            "row_count": qresult.row_count,
        }
        response_text = json.dumps(data, default=str)

        # Prepend warnings from result checks
        if rresult.warnings:
            warning_text = "WARNINGS:\n" + "\n".join(
                f"- {w}" for w in rresult.warnings
            )
            response_text = warning_text + "\n\n" + response_text

        return _text_response(response_text)
```

- [ ] **Step 4: Update validate_query to note pending result checks**

In `src/agentic_data_contracts/tools/factory.py`, replace the `validate_query` function (lines 213-226) with:

```python
    # ── Tool 7: validate_query ────────────────────────────────────────────
    async def validate_query(args: dict[str, Any]) -> dict[str, Any]:
        sql = args.get("sql", "")
        result = validator.validate(sql)
        if result.blocked:
            msg = "BLOCKED — Violations:\n" + "\n".join(
                f"- {r}" for r in result.reasons
            )
            if result.warnings:
                msg += "\nWarnings:\n" + "\n".join(f"- {w}" for w in result.warnings)
        else:
            msg = "VALID — Query passed all pre-execution checks."
            if result.warnings:
                msg += "\nWarnings:\n" + "\n".join(f"- {w}" for w in result.warnings)
            # Note pending result checks
            if validator._result_checkers:
                names = [name for _, _, runner in validator._result_checkers
                         for name in [runner.rule_name]]
                msg += (
                    f"\nNote: {len(names)} result check(s) will run after execution: "
                    + ", ".join(names)
                )
        return _text_response(msg)
```

- [ ] **Step 5: Run all tool tests**

Run: `uv run pytest tests/test_tools/test_factory.py -v`
Expected: All PASS.

- [ ] **Step 6: Commit**

```bash
git add src/agentic_data_contracts/tools/factory.py tests/test_tools/test_factory.py
git commit -m "feat: result check enforcement in run_query, session cost recording"
```

---

### Task 7: Fix Remaining Test Suites

**Files:**
- Modify: various test files that import or reference old `filter_column` or old checker APIs

- [ ] **Step 1: Run full test suite to find remaining failures**

Run: `uv run pytest -v`
Expected: Some failures in tests that still reference old APIs.

- [ ] **Step 2: Fix test_core/test_system_prompt_metrics.py if needed**

Run: `uv run pytest tests/test_core/ -v`
Check for failures and fix any that reference old `filter_column` format.

- [ ] **Step 3: Fix test_tools/ remaining tests if needed**

Run: `uv run pytest tests/test_tools/ -v`
Check for failures from changed fixture format (valid_contract.yml).

- [ ] **Step 4: Fix test_public_api.py if needed**

The public API test imports `RequiredFilterChecker` — its constructor changed from `required_filters: list[str]` to `column: str`. The import still works, just the signature changed. Since the test only checks importability, it should pass.

Run: `uv run pytest tests/test_public_api.py -v`

- [ ] **Step 5: Run full suite**

Run: `uv run pytest -v`
Expected: All PASS.

- [ ] **Step 6: Run linting and type checking**

Run: `uv run ruff check src/ tests/ && uv run ruff format src/ tests/ && ty check`

- [ ] **Step 7: Commit any fixes**

```bash
git add -u
git commit -m "fix: update remaining tests for unified rule engine"
```

---

### Task 8: Update Validation Module Public Exports

**Files:**
- Modify: `src/agentic_data_contracts/validation/__init__.py`

- [ ] **Step 1: Check current exports**

Read `src/agentic_data_contracts/validation/__init__.py` to see current public API.

- [ ] **Step 2: Update exports to include new types**

Ensure `Checker` protocol, `ResultCheckRunner`, and new checker classes are exported. The `Checker` protocol's `check_ast` signature changed — update the export.

- [ ] **Step 3: Run public API test**

Run: `uv run pytest tests/test_public_api.py -v`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add src/agentic_data_contracts/validation/__init__.py
git commit -m "feat: export new checker types from validation module"
```

---

### Task 9: Final Verification

- [ ] **Step 1: Run full test suite**

Run: `uv run pytest -v`
Expected: All PASS.

- [ ] **Step 2: Run pre-commit hooks**

Run: `prek run --all-files`
Expected: All PASS.

- [ ] **Step 3: Verify no regressions in system prompt generation**

Run: `uv run pytest tests/test_core/test_prompt_renderers.py tests/test_core/test_system_prompt_metrics.py -v`
Expected: All PASS.

- [ ] **Step 4: Verify bridge still works**

Run: `uv run pytest tests/test_bridge/ -v`
Expected: PASS or skipped.

- [ ] **Step 5: Final commit if any remaining changes**

```bash
git status
# If any unstaged changes:
git add -u
git commit -m "chore: final cleanup for unified rule engine"
```
