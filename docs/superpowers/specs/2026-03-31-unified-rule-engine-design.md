# Unified Rule Engine: Query Checks, Result Checks, and Session Cost Enforcement

**Date:** 2026-03-31
**Status:** Approved
**Motivation:** Inspired by Meta's AI Analytics Agent architecture — extend our governance layer with post-execution result validation, declarative pre-execution query checks, and consistent session cost enforcement.

## Summary

Redesign the rule system from hardcoded checker construction to a rules-driven, three-phase validation pipeline. All rules live in one unified `rules` list in the YAML contract. Each rule declares **what** constraint it enforces, **where** it applies (table scoping), and **how** to enforce it (block/warn/log). The engine determines *when* to run each rule based on whether it carries a `query_check` (pre-execution) or `result_check` (post-execution) block.

## Design Principles

1. **Unified mental model** — contract authors write rules in one place; the engine figures out execution phase.
2. **Declarative built-ins first** — no custom Python class loading in v1; support the most common checks as built-in fields.
3. **Table scoping everywhere** — every rule can be scoped to a specific table or applied globally.
4. **Efficient parsing** — parse the SQL AST once, pass it to all applicable checkers.
5. **No backward compatibility constraints** — no existing users, so we clean up legacy patterns.

## YAML Schema

### Rule Model

```yaml
rules:
  - name: <string>            # unique rule name
    description: <string>     # human-readable description
    enforcement: block|warn|log
    table: <string>|null      # "schema.table" or "*" or omitted (= global)
    query_check: <QueryCheck>|null   # pre-execution check
    result_check: <ResultCheck>|null # post-execution check
```

A rule must have at most one of `query_check` or `result_check` (not both). Rules with neither are **advisory** — they appear in the system prompt as guidance but don't enforce anything.

### QueryCheck Fields

| Field | Type | Description |
|---|---|---|
| `required_filter` | `str` | Column name that must appear in a WHERE clause |
| `no_select_star` | `bool` | Forbid `SELECT *` anywhere in the query |
| `blocked_columns` | `list[str]` | Columns that must not appear in SELECT |
| `require_limit` | `bool` | Query must include a LIMIT clause |
| `max_joins` | `int` | Maximum number of JOINs allowed |

Multiple fields can be set on a single `query_check` — all must pass.

### ResultCheck Fields

| Field | Type | Description |
|---|---|---|
| `column` | `str` | Column to check (required for value/null checks, not for row checks) |
| `min_value` | `float` | Minimum allowed value in the column |
| `max_value` | `float` | Maximum allowed value in the column |
| `not_null` | `bool` | Column must not contain any null values |
| `min_rows` | `int` | Minimum number of rows in result set |
| `max_rows` | `int` | Maximum number of rows in result set |

Multiple fields can be set on a single `result_check` — all must pass.

### Full YAML Example

```yaml
version: "1.0"
name: analytics-agent

semantic:
  source:
    type: yaml
    path: semantic.yml

  allowed_tables:
    - schema: analytics
      tables: [orders, customers, user_metrics, dim_countries]

  forbidden_operations: [DELETE, DROP, TRUNCATE, UPDATE, INSERT]

  rules:
    # Pre-execution: query structure checks
    - name: tenant_isolation
      description: "Orders must be filtered by tenant_id"
      enforcement: block
      table: "analytics.orders"
      query_check:
        required_filter: tenant_id

    - name: no_select_star
      description: "Require explicit column selection"
      enforcement: warn
      query_check:
        no_select_star: true

    - name: hide_pii
      description: "Do not select PII columns from customers"
      enforcement: block
      table: "analytics.customers"
      query_check:
        blocked_columns: [ssn, email, phone]

    - name: must_have_limit
      description: "All queries must include a LIMIT"
      enforcement: block
      query_check:
        require_limit: true

    - name: limit_complexity
      description: "Queries should not be overly complex"
      enforcement: warn
      query_check:
        max_joins: 3

    # Post-execution: result value checks
    - name: wau_sanity
      description: "WAU should not exceed world population"
      enforcement: warn
      table: "analytics.user_metrics"
      result_check:
        column: wau
        max_value: 8_000_000_000

    - name: no_negative_revenue
      description: "Revenue must not be negative"
      enforcement: block
      result_check:
        column: revenue
        min_value: 0

    - name: result_not_empty
      description: "Warn if query returns no rows"
      enforcement: warn
      result_check:
        min_rows: 1

    - name: result_size_limit
      description: "Block excessively large result sets"
      enforcement: block
      result_check:
        max_rows: 10000

resources:
  cost_limit_usd: 5.0
  max_retries: 5
  max_rows_scanned: 1_000_000
```

## Pydantic Models

### Removed

- `SemanticRule.filter_column` — replaced by `query_check.required_filter`

### New Models

```python
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
            raise ValueError("Rule must not have both query_check and result_check")
        return self
```

## Checker Architecture

### Checker Protocol (revised)

```python
class Checker(Protocol):
    def check_ast(self, ast: exp.Expression, contract: DataContract) -> CheckResult: ...
```

All pre-execution checkers receive a pre-parsed sqlglot AST instead of raw SQL. The `Validator` parses once. This includes `TableAllowlistChecker` and `OperationBlocklistChecker` — they are refactored from `check_sql` to `check_ast` as well.

### Table Scoping

The `Validator` extracts referenced tables from the AST before running checkers. For a rule with `table` set:
- If the rule's table is in the query's referenced tables → run the checker
- If not → skip (the rule doesn't apply to this query)
- If `table` is `None` or `"*"` → always run

The existing `TableAllowlistChecker._extract_tables()` logic is extracted into a shared utility.

### Built-in Query Checkers

Each `query_check` field maps to a checker class:

| Field | Checker | Logic |
|---|---|---|
| `required_filter` | `RequiredFilterChecker` | Existing — scan WHERE clauses for column name |
| `no_select_star` | `NoSelectStarChecker` | Existing — find `exp.Star` in AST |
| `blocked_columns` | `BlockedColumnsChecker` | **New** — find `exp.Column` nodes in SELECT, check against blocklist |
| `require_limit` | `RequireLimitChecker` | **New** — check for `exp.Limit` node in AST |
| `max_joins` | `MaxJoinsChecker` | **New** — count `exp.Join` nodes in AST |

### Built-in Result Checkers

Result checkers don't use sqlglot — they operate on `columns: list[str]` and `rows: list[tuple]`.

```python
class ResultChecker(Protocol):
    def check_results(self, columns: list[str], rows: list[tuple]) -> CheckResult: ...
```

A single `ResultCheckRunner` class handles all `result_check` fields:

```python
class ResultCheckRunner:
    def __init__(self, config: ResultCheck, rule_name: str) -> None: ...
    def check_results(self, columns: list[str], rows: list[tuple]) -> CheckResult: ...
```

Internally dispatches to the right logic based on which fields are set. Violation messages include the actual violating values:
- `"Rule 'wau_sanity': column 'wau' max value 12,000,000,000 exceeds limit 8,000,000,000"`
- `"Rule 'result_not_empty': query returned 0 rows, minimum is 1"`

## Validator Changes

### `Validator.__init__`

```python
def __init__(self, contract, dialect=None, explain_adapter=None):
    self.contract = contract
    self.dialect = dialect
    self.explain_adapter = explain_adapter
    # Built from rules with query_check
    self._query_checkers: list[tuple[str, str | None, Checker]] = ...  # (enforcement, table, checker)
    # Built from rules with result_check
    self._result_checkers: list[tuple[str, str | None, ResultCheckRunner]] = ...  # (enforcement, table, runner)
```

### `Validator.validate(sql)` — Phases 1 + 2

```
1. Parse SQL to AST (once)
2. Extract referenced tables from AST
3. Run TableAllowlistChecker (from allowed_tables config)
4. Run OperationBlocklistChecker (from forbidden_operations config)
5. For each query_check rule:
   a. Check table scoping — skip if rule's table not in referenced tables
   b. Run checker against AST
   c. Route result to reasons/warnings/log_messages by enforcement
6. If no blocks and explain_adapter is available:
   a. Run EXPLAIN
   b. Check cost/row limits from resources config
   c. Pass estimated_cost_usd through in ValidationResult
7. Return ValidationResult
```

### `Validator.validate_results(sql, columns, rows)` — Phase 3

Note: this method re-parses the SQL to extract referenced tables for scoping. This is a cheap operation and keeps `validate()` and `validate_results()` independent — no shared state needs to be threaded between them.

```
1. Parse SQL to AST (to extract referenced tables for scoping)
2. Extract referenced tables
3. For each result_check rule:
   a. Check table scoping — skip if rule's table not in referenced tables
   b. Run ResultCheckRunner against (columns, rows)
   c. Route result to reasons/warnings/log_messages by enforcement
4. Return ValidationResult
```

### `ValidationResult` (extended)

```python
@dataclass
class ValidationResult:
    blocked: bool
    reasons: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    log_messages: list[str] = field(default_factory=list)
    estimated_cost_usd: float | None = None  # passed through from EXPLAIN
```

## Tool Layer Changes

### `run_query` — Updated Flow

```python
async def run_query(args):
    sql = args["sql"]

    # Check session limits
    session.check_limits()

    # Phase 1 + 2: query checks + EXPLAIN
    vresult = validator.validate(sql)
    if vresult.blocked:
        session.record_retry()
        return blocked_response(vresult)

    # Record estimated cost from EXPLAIN
    if vresult.estimated_cost_usd is not None:
        session.record_cost(vresult.estimated_cost_usd)

    # Execute
    qresult = adapter.execute(sql)

    # Phase 3: result checks
    rresult = validator.validate_results(sql, qresult.columns, qresult.rows)
    if rresult.blocked:
        session.record_retry()
        return blocked_response(rresult)  # data discarded, agent sees violation + violating values

    # Return results + any warnings
    response = format_results(qresult)
    if rresult.warnings:
        response = prepend_warnings(rresult.warnings, response)
    return response
```

### `validate_query` — No Change in Interface

Still runs Phase 1 + 2 only (no results to check). Output now includes a note listing which result_check rules will apply at execution time, so the agent is aware of them.

### No New Tools

The 10-tool interface remains unchanged. Result checks are enforced transparently inside `run_query`.

## Session Cost Enforcement

### The Gap

`ContractSession` already tracks `cost_usd` and `check_limits()` compares against `cost_limit_usd`. But `run_query` never called `session.record_cost()`.

### The Fix

After Phase 2 validation, if `ValidationResult.estimated_cost_usd` is set, `run_query` records it via `session.record_cost()`. On subsequent queries, `session.check_limits()` (which runs first) will block if cumulative cost exceeds the budget.

For databases without cost estimates (e.g., DuckDB returns row estimates only), cost tracking is skipped. This is correct — if you can't estimate cost, you can't enforce a cost budget.

## Files Changed

| File | Change |
|---|---|
| `core/schema.py` | Add `QueryCheck`, `ResultCheck` models. Remove `filter_column` from `SemanticRule`. Add `table`, `query_check`, `result_check` fields with model validator. |
| `validation/checkers.py` | Refactor all checkers to `check_ast()` protocol. Add `BlockedColumnsChecker`, `RequireLimitChecker`, `MaxJoinsChecker`. Add `ResultCheckRunner`. Extract `extract_tables()` utility. |
| `validation/validator.py` | Rewrite `_build_checkers()` to be rules-driven. Add `validate_results()` method. Pass `estimated_cost_usd` through `ValidationResult`. Parse AST once. |
| `tools/factory.py` | Update `run_query` to call `validate_results()` after execution and `session.record_cost()`. Update `validate_query` to note pending result checks. |
| `core/session.py` | No structural changes — `record_cost()` already exists. |
| `core/contract.py` | Remove `block_rules()` helper (no longer needed — validator reads rules directly). |
| `tests/fixtures/*.yml` | Update all YAML fixtures to new rule format. |
| `tests/test_validation/` | New tests for each checker, result checks, table scoping, session cost flow. |
| `tests/test_core/` | Update schema tests for new models. |
| `tests/test_tools/` | Update tool tests for result check enforcement in `run_query`. |

## Testing Strategy

1. **Unit tests per checker** — each query checker tested in isolation with AST input.
2. **Unit tests for ResultCheckRunner** — each result_check field tested with mock columns/rows.
3. **Table scoping tests** — rules with/without table scoping, wildcard, non-matching tables.
4. **Validator integration tests** — full pipeline with mixed query_check and result_check rules.
5. **Tool integration tests** — `run_query` with result checks that block, warn, and pass.
6. **Session cost tests** — cumulative cost tracking across multiple `run_query` calls.
7. **YAML parsing tests** — new models, validation (exactly one of query_check/result_check), edge cases.
8. **Migration tests** — ensure old fixtures fail fast with clear errors (no silent misparse).
