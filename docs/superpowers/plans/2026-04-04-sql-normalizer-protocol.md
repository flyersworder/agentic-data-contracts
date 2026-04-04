# SqlNormalizer Protocol Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an optional `SqlNormalizer` protocol so adapters for non-standard SQL dialects can rewrite queries before AST validation, without affecting existing adapters.

**Architecture:** New `@runtime_checkable` protocol `SqlNormalizer` with a single `normalize_sql` method. The `Validator` accepts an optional normalizer and calls it before `sqlglot.parse_one()`. Integration points (`factory.py`, `middleware.py`) detect the protocol via `isinstance()` and wire it in automatically.

**Tech Stack:** Python 3.12+, Pydantic 2, sqlglot, pytest

---

### Task 1: Define the `SqlNormalizer` protocol

**Files:**
- Modify: `src/agentic_data_contracts/adapters/base.py:35-44`

- [ ] **Step 1: Add the `SqlNormalizer` protocol after `DatabaseAdapter`**

Add the following after line 44 of `src/agentic_data_contracts/adapters/base.py`:

```python
@runtime_checkable
class SqlNormalizer(Protocol):
    """Rewrite database-specific SQL into a form sqlglot can parse.

    Called by the Validator before AST parsing. Adapters for non-standard
    dialects implement this alongside DatabaseAdapter. Standard-dialect
    adapters do not need to implement this — the Validator treats its
    absence as a no-op.

    The original (un-normalized) SQL is still passed to execute() and explain().
    """

    def normalize_sql(self, sql: str) -> str: ...
```

- [ ] **Step 2: Verify the file is syntactically valid**

Run: `uv run python -c "from agentic_data_contracts.adapters.base import SqlNormalizer; print(SqlNormalizer)"`
Expected: `<class 'agentic_data_contracts.adapters.base.SqlNormalizer'>`

- [ ] **Step 3: Commit**

```bash
git add src/agentic_data_contracts/adapters/base.py
git commit -m "feat: add SqlNormalizer protocol to adapters"
```

---

### Task 2: Wire `SqlNormalizer` into the `Validator`

**Files:**
- Modify: `src/agentic_data_contracts/validation/validator.py:1-60` (imports + `__init__`)
- Modify: `src/agentic_data_contracts/validation/validator.py:150-159` (`validate` method)

- [ ] **Step 1: Add the import**

In `src/agentic_data_contracts/validation/validator.py`, add this import after the existing `ExplainAdapter` import on line 24:

```python
from agentic_data_contracts.adapters.base import SqlNormalizer
```

- [ ] **Step 2: Add `sql_normalizer` parameter to `Validator.__init__`**

Change lines 51-60 from:

```python
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
```

to:

```python
    def __init__(
        self,
        contract: DataContract,
        dialect: str | None = None,
        explain_adapter: ExplainAdapter | None = None,
        sql_normalizer: SqlNormalizer | None = None,
    ) -> None:
        self.contract = contract
        self.dialect = dialect
        self.explain_adapter = explain_adapter
        self.sql_normalizer = sql_normalizer
        self._build_checkers()
```

- [ ] **Step 3: Call `normalize_sql` before `sqlglot.parse_one()` in `validate()`**

Change line 157 from:

```python
            ast = cast(exp.Expression, sqlglot.parse_one(sql, dialect=self.dialect))
```

to:

```python
            normalized = self.sql_normalizer.normalize_sql(sql) if self.sql_normalizer else sql
            ast = cast(exp.Expression, sqlglot.parse_one(normalized, dialect=self.dialect))
```

Note: `explain_adapter.explain(sql)` on line 186 keeps using the original `sql` — this is intentional, since the database understands its own dialect.

- [ ] **Step 4: Verify existing tests still pass**

Run: `uv run pytest tests/test_validation/test_validator.py -v`
Expected: All existing tests pass (normalizer is `None` by default, so behavior is unchanged).

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/validation/validator.py
git commit -m "feat: wire SqlNormalizer into Validator.validate()"
```

---

### Task 3: Wire `SqlNormalizer` into integration points

**Files:**
- Modify: `src/agentic_data_contracts/tools/factory.py:48-49`
- Modify: `src/agentic_data_contracts/tools/middleware.py:9,24-25`

- [ ] **Step 1: Update `factory.py` to detect and pass `SqlNormalizer`**

In `src/agentic_data_contracts/tools/factory.py`, add the import after the existing `DatabaseAdapter` import on line 9:

```python
from agentic_data_contracts.adapters.base import DatabaseAdapter, SqlNormalizer
```

Then change lines 48-49 from:

```python
    dialect = adapter.dialect if adapter else None
    validator = Validator(contract, dialect=dialect, explain_adapter=adapter)
```

to:

```python
    dialect = adapter.dialect if adapter else None
    sql_normalizer = adapter if isinstance(adapter, SqlNormalizer) else None
    validator = Validator(contract, dialect=dialect, explain_adapter=adapter, sql_normalizer=sql_normalizer)
```

- [ ] **Step 2: Update `middleware.py` to detect and pass `SqlNormalizer`**

In `src/agentic_data_contracts/tools/middleware.py`, change the import on line 9 from:

```python
from agentic_data_contracts.adapters.base import DatabaseAdapter
```

to:

```python
from agentic_data_contracts.adapters.base import DatabaseAdapter, SqlNormalizer
```

Then change lines 24-25 from:

```python
    dialect = adapter.dialect if adapter else None
    validator = Validator(contract, dialect=dialect, explain_adapter=adapter)
```

to:

```python
    dialect = adapter.dialect if adapter else None
    sql_normalizer = adapter if isinstance(adapter, SqlNormalizer) else None
    validator = Validator(contract, dialect=dialect, explain_adapter=adapter, sql_normalizer=sql_normalizer)
```

- [ ] **Step 3: Verify all tests still pass**

Run: `uv run pytest -v`
Expected: Full test suite passes — no behavioral change for adapters that don't implement `SqlNormalizer`.

- [ ] **Step 4: Commit**

```bash
git add src/agentic_data_contracts/tools/factory.py src/agentic_data_contracts/tools/middleware.py
git commit -m "feat: detect SqlNormalizer in factory and middleware"
```

---

### Task 4: Add exports

**Files:**
- Modify: `src/agentic_data_contracts/adapters/__init__.py`
- Modify: `src/agentic_data_contracts/__init__.py:1-16`

- [ ] **Step 1: Export from adapters package**

Write `src/agentic_data_contracts/adapters/__init__.py`:

```python
from agentic_data_contracts.adapters.base import (
    Column,
    DatabaseAdapter,
    QueryResult,
    SqlNormalizer,
    TableSchema,
)

__all__ = [
    "Column",
    "DatabaseAdapter",
    "QueryResult",
    "SqlNormalizer",
    "TableSchema",
]
```

- [ ] **Step 2: Export from package root**

In `src/agentic_data_contracts/__init__.py`, add the `SqlNormalizer` import and export:

```python
"""Agentic Data Contracts — YAML-first data contract governance for AI agents."""

from agentic_data_contracts.adapters.base import SqlNormalizer
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.prompt import ClaudePromptRenderer, PromptRenderer
from agentic_data_contracts.tools.factory import create_tools
from agentic_data_contracts.tools.middleware import contract_middleware
from agentic_data_contracts.tools.sdk import create_sdk_mcp_server

__all__ = [
    "ClaudePromptRenderer",
    "DataContract",
    "PromptRenderer",
    "SqlNormalizer",
    "contract_middleware",
    "create_sdk_mcp_server",
    "create_tools",
]
```

- [ ] **Step 3: Verify import works**

Run: `uv run python -c "from agentic_data_contracts import SqlNormalizer; print(SqlNormalizer)"`
Expected: `<class 'agentic_data_contracts.adapters.base.SqlNormalizer'>`

- [ ] **Step 4: Commit**

```bash
git add src/agentic_data_contracts/adapters/__init__.py src/agentic_data_contracts/__init__.py
git commit -m "feat: export SqlNormalizer from adapters and package root"
```

---

### Task 5: Write tests for `SqlNormalizer`

**Files:**
- Create: `tests/test_validation/test_sql_normalizer.py`

- [ ] **Step 1: Write the test file**

Create `tests/test_validation/test_sql_normalizer.py`:

```python
"""Tests for the SqlNormalizer protocol hook."""

from __future__ import annotations

import re

from agentic_data_contracts.adapters.base import (
    DatabaseAdapter,
    QueryResult,
    SqlNormalizer,
    TableSchema,
)
from agentic_data_contracts.core.schema import (
    AllowedTable,
    DataContractSchema,
    SemanticConfig,
)
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.validation.explain import ExplainAdapter, ExplainResult
from agentic_data_contracts.validation.validator import Validator


class VqlNormalizingAdapter:
    """Mock adapter that normalizes VQL CAST('type', col) to CAST(col AS type)."""

    @property
    def dialect(self) -> str:
        return "postgres"

    def normalize_sql(self, sql: str) -> str:
        # Rewrite CAST('type', col) -> CAST(col AS type)
        return re.sub(
            r"CAST\('(\w+)',\s*(\w+)\)",
            r"CAST(\2 AS \1)",
            sql,
        )

    def execute(self, sql: str) -> QueryResult:
        return QueryResult(columns=["id"], rows=[(1,)])

    def explain(self, sql: str) -> ExplainResult:
        return ExplainResult(
            estimated_cost_usd=0.01,
            estimated_rows=100,
            schema_valid=True,
        )

    def describe_table(self, schema: str, table: str) -> TableSchema:
        return TableSchema()

    def list_tables(self, schema: str) -> list[str]:
        return []


class PlainAdapter:
    """Mock adapter without SqlNormalizer — standard dialect."""

    @property
    def dialect(self) -> str:
        return "duckdb"

    def execute(self, sql: str) -> QueryResult:
        return QueryResult(columns=["id"], rows=[(1,)])

    def explain(self, sql: str) -> ExplainResult:
        return ExplainResult(
            estimated_cost_usd=0.01,
            estimated_rows=100,
            schema_valid=True,
        )

    def describe_table(self, schema: str, table: str) -> TableSchema:
        return TableSchema()

    def list_tables(self, schema: str) -> list[str]:
        return []


def _make_contract() -> DataContract:
    schema = DataContractSchema(
        version="1.0",
        name="normalizer-test",
        semantic=SemanticConfig(
            allowed_tables=[AllowedTable(schema_="public", tables=["users"])],
            forbidden_operations=[],
            rules=[],
        ),
    )
    return DataContract(schema=schema)


# --- Protocol detection ---


def test_normalizing_adapter_is_sql_normalizer() -> None:
    adapter = VqlNormalizingAdapter()
    assert isinstance(adapter, SqlNormalizer)


def test_normalizing_adapter_is_database_adapter() -> None:
    adapter = VqlNormalizingAdapter()
    assert isinstance(adapter, DatabaseAdapter)


def test_plain_adapter_is_not_sql_normalizer() -> None:
    adapter = PlainAdapter()
    assert not isinstance(adapter, SqlNormalizer)


# --- Validation with normalization ---


def test_vql_cast_passes_with_normalizer() -> None:
    """VQL CAST('varchar', col) is invalid sqlglot input but normalizes to valid SQL."""
    adapter = VqlNormalizingAdapter()
    contract = _make_contract()
    validator = Validator(
        contract,
        dialect=adapter.dialect,
        sql_normalizer=adapter,
    )
    result = validator.validate(
        "SELECT CAST('varchar', id) FROM public.users"
    )
    assert not result.blocked


def test_vql_cast_fails_without_normalizer() -> None:
    """Without normalization, VQL CAST syntax causes a parse error."""
    contract = _make_contract()
    validator = Validator(contract, dialect="postgres")
    result = validator.validate(
        "SELECT CAST('varchar', id) FROM public.users"
    )
    assert result.blocked
    assert any("parse error" in r.lower() or "CAST" in r for r in result.reasons)


# --- Original SQL preserved for explain ---


def test_explain_receives_original_sql() -> None:
    """The explain adapter must receive the original SQL, not the normalized form."""
    received_sql: list[str] = []

    class SpyNormalizingAdapter(VqlNormalizingAdapter):
        def explain(self, sql: str) -> ExplainResult:
            received_sql.append(sql)
            return super().explain(sql)

    adapter = SpyNormalizingAdapter()
    contract = _make_contract()
    validator = Validator(
        contract,
        dialect=adapter.dialect,
        explain_adapter=adapter,
        sql_normalizer=adapter,
    )
    original_sql = "SELECT CAST('varchar', id) FROM public.users"
    validator.validate(original_sql)
    assert received_sql == [original_sql]
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `uv run pytest tests/test_validation/test_sql_normalizer.py -v`
Expected: All 6 tests pass.

- [ ] **Step 3: Run full test suite**

Run: `uv run pytest -v`
Expected: Full suite passes, no regressions.

- [ ] **Step 4: Commit**

```bash
git add tests/test_validation/test_sql_normalizer.py
git commit -m "test: add SqlNormalizer protocol tests"
```

---

### Task 6: Lint and type check

**Files:** None (verification only)

- [ ] **Step 1: Run ruff check**

Run: `uv run ruff check src/ tests/`
Expected: No errors.

- [ ] **Step 2: Run ruff format**

Run: `uv run ruff format src/ tests/`
Expected: No changes (or auto-formatted).

- [ ] **Step 3: Run type checker**

Run: `ty check`
Expected: No new errors.

- [ ] **Step 4: Commit any formatting fixes**

If ruff format made changes:
```bash
git add -u
git commit -m "style: format SqlNormalizer code"
```
