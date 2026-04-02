# SQL Pre-Processing Hook: `SqlNormalizer` Protocol

**Date:** 2026-04-02
**Status:** Approved

## Problem

The validation pipeline passes raw SQL directly to `sqlglot.parse_one(sql, dialect=self.dialect)`. This works for databases whose syntax sqlglot understands natively (DuckDB, Postgres, BigQuery, Snowflake), but breaks for databases with non-standard SQL extensions like Denodo VQL, Teradata, or Oracle PL/SQL.

The adapter's `execute()` and `explain()` handle these dialects fine since they pass SQL directly to the database, but the AST-based validation step (Layer 1) is unreachable.

## Solution

A new `@runtime_checkable` protocol, `SqlNormalizer`, with a single method `normalize_sql(self, sql: str) -> str`. The Validator calls it before AST parsing. Adapters for non-standard dialects implement both `DatabaseAdapter` and `SqlNormalizer`; standard-dialect adapters are unaffected.

This follows the existing pattern: `ExplainAdapter` is already a separate protocol from `DatabaseAdapter`, used via `isinstance()` checks at the integration points.

## Design

### New Protocol

Location: `src/agentic_data_contracts/adapters/base.py`

```python
@runtime_checkable
class SqlNormalizer(Protocol):
    def normalize_sql(self, sql: str) -> str:
        """Rewrite database-specific SQL into a form sqlglot can parse.

        Called by the Validator before AST parsing. Adapters for non-standard
        dialects override this to convert proprietary syntax into the closest
        standard equivalent.

        The original (un-normalized) SQL is still passed to execute() and explain().
        """
        ...
```

### Validator Changes

Location: `src/agentic_data_contracts/validation/validator.py`

1. `Validator.__init__` gains `sql_normalizer: SqlNormalizer | None = None`
2. In `validate()`, before `sqlglot.parse_one()`:

```python
normalized = self.sql_normalizer.normalize_sql(sql) if self.sql_normalizer else sql
ast = cast(exp.Expression, sqlglot.parse_one(normalized, dialect=self.dialect))
```

3. The original `sql` (not `normalized`) is still passed to `explain_adapter.explain(sql)`, since the database understands its own dialect.

### Integration Points

Three places create a `Validator` and need to detect `SqlNormalizer`:

1. **`tools/factory.py` line 49** — `create_tools()` already has `adapter`. Add:
   ```python
   sql_normalizer = adapter if isinstance(adapter, SqlNormalizer) else None
   validator = Validator(contract, dialect=dialect, explain_adapter=adapter, sql_normalizer=sql_normalizer)
   ```

2. **`tools/middleware.py` line 25** — `contract_middleware()` already has `adapter`. Same pattern.

3. **Direct `Validator` construction** — users constructing `Validator` manually can pass `sql_normalizer` explicitly.

### Tools Layer

No changes needed. `validate_query` and `run_query` call `validator.validate(sql)`, which handles normalization internally. `query_cost_estimate` calls `adapter.explain(sql)` directly with the original SQL — correct, since the database understands its own dialect.

### Exports

- `SqlNormalizer` exported from `adapters/__init__.py`
- `SqlNormalizer` exported from the package root `__init__.py`

## Testing

A test in `tests/test_validation/test_sql_normalizer.py` with:

1. A mock adapter implementing both `DatabaseAdapter` and `SqlNormalizer` that rewrites a VQL-like `CAST('varchar', col)` into `CAST(col AS varchar)`
2. Test that `validator.validate()` succeeds on the VQL form (normalization applied)
3. Test that without the normalizer, the same VQL form would fail to parse or produce incorrect AST
4. Test that the original SQL is passed to `explain()`, not the normalized form
5. Test that `isinstance(mock_adapter, SqlNormalizer)` is `True` (runtime checkable)
6. Test that `isinstance(duckdb_adapter, SqlNormalizer)` is `False` (standard adapters unaffected)

## Scope Boundaries

- No Denodo adapter implementation — that's a separate effort
- No changes to `ExplainAdapter` or `DatabaseAdapter` protocols
- No async support — `normalize_sql` is sync, matching the existing protocol style
- No documentation changes beyond this spec (adapter authoring guide update deferred)
