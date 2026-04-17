# Tool Surface Consolidation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce the agent tool surface from 13 tools to 9 by dropping tools fully redundant with `ClaudePromptRenderer` and merging `validate_query` + `query_cost_estimate` into a single `inspect_query` tool.

**Architecture:** Additive work first (extend `ValidationResult`, add `inspect_query`, extend `run_query` response with session budget), then destructive work (remove 5 tools and prune affected tests), then docs and release metadata.

**Tech Stack:** Python 3.12+, Pydantic 2, sqlglot, pytest + pytest-asyncio, DuckDB (test fixture), uv, ruff, ty.

---

## Reference: Files Touched

**Source:**
- `src/agentic_data_contracts/validation/validator.py` — extend `ValidationResult` dataclass; populate new fields in `Validator.validate()`
- `src/agentic_data_contracts/tools/factory.py` — add `inspect_query`; extend `run_query`; remove 5 tools
- `src/agentic_data_contracts/tools/sdk.py` — docstring count fix

**Tests:**
- `tests/test_tools/test_inspect_query.py` — new
- `tests/test_tools/test_factory.py` — update count assertion, drop obsolete tests
- `tests/test_tools/test_pagination.py` — delete
- `tests/test_tools/test_wildcard_tools.py` — drop/rewrite 2 tests
- `tests/test_tools/test_semantic_tools.py` — drop 2 tests
- `tests/test_validation/test_validator.py` — extend (if present) or add coverage for new fields

**Docs/release:**
- `README.md` — tool table, quickstart, count
- `docs/architecture.md` — numbered tool list, workflow, count
- `CHANGELOG.md` — 0.11.0 entry
- `pyproject.toml` — version bump to 0.11.0

**Not touched** (false positives — these reference `DatabaseAdapter.list_tables()`, the protocol method, not the tool):
- `tests/test_core/test_scalability.py`
- `tests/test_core/test_wildcard_tables.py`
- `tests/test_validation/test_sql_normalizer.py`
- `src/agentic_data_contracts/core/contract.py`
- `src/agentic_data_contracts/adapters/{base,duckdb}.py`

---

## Task 1: Extend `ValidationResult` with EXPLAIN fields

**Rationale:** `inspect_query` needs to surface `estimated_rows`, `schema_valid`, and raw EXPLAIN errors. Today the validator consumes EXPLAIN internally and only exposes `estimated_cost_usd`. Adding three fields to the dataclass (a non-protocol type) is additive and breaks nothing.

**Files:**
- Modify: `src/agentic_data_contracts/validation/validator.py:44-50` (dataclass) and `:208-245` (populate + return)

- [ ] **Step 1: Extend the `ValidationResult` dataclass**

Replace lines 44–50 with:

```python
@dataclass
class ValidationResult:
    blocked: bool
    reasons: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    log_messages: list[str] = field(default_factory=list)
    estimated_cost_usd: float | None = None
    estimated_rows: int | None = None
    schema_valid: bool = True
    explain_errors: list[str] = field(default_factory=list)
```

- [ ] **Step 2: Populate new fields inside `Validator.validate()`**

Inside `validate()`, introduce two locals at the top next to `estimated_cost_usd`:

```python
estimated_rows: int | None = None
schema_valid: bool = True
explain_errors: list[str] = []
```

In the EXPLAIN branch starting at the current line 208, after the `explain_result = self.explain_adapter.explain(sql)` call, assign:

```python
schema_valid = explain_result.schema_valid
explain_errors = list(explain_result.errors)
```

Still inside the EXPLAIN branch, in the `else` clause (when `schema_valid` is true), set:

```python
estimated_rows = explain_result.estimated_rows
```

alongside the existing `estimated_cost_usd` assignment.

- [ ] **Step 3: Update the `return ValidationResult(...)` at the end of `validate()`**

Replace lines 239–245 with:

```python
return ValidationResult(
    blocked=len(reasons) > 0,
    reasons=reasons,
    warnings=warnings,
    log_messages=log_messages,
    estimated_cost_usd=estimated_cost_usd,
    estimated_rows=estimated_rows,
    schema_valid=schema_valid,
    explain_errors=explain_errors,
)
```

Leave `validate_results()` unchanged — its `ValidationResult` uses defaults for the new fields.

- [ ] **Step 4: Run the existing validator suite to confirm no regression**

Run: `uv run pytest tests/test_validation -v`
Expected: all tests pass (the new fields default to safe values for existing callers).

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/validation/validator.py
git commit -m "feat(validator): surface estimated_rows, schema_valid, explain_errors in ValidationResult"
```

---

## Task 2: Add the `inspect_query` tool (TDD)

**Rationale:** Merges the old `validate_query` + `query_cost_estimate` into one read-only SQL inspection tool. Must run without a database adapter (Layer 1 only) and with one (Layer 1 + EXPLAIN).

**Files:**
- Create: `tests/test_tools/test_inspect_query.py`
- Modify: `src/agentic_data_contracts/tools/factory.py` (add function + `ToolDef`)

- [ ] **Step 1: Write the failing tests**

Create `tests/test_tools/test_inspect_query.py`:

```python
"""Tests for the inspect_query tool (merge of validate_query + query_cost_estimate)."""

import json
from pathlib import Path

import pytest

from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.semantic.yaml_source import YamlSource
from agentic_data_contracts.tools.factory import create_tools


@pytest.fixture
def contract(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml(fixtures_dir / "valid_contract.yml")


@pytest.fixture
def adapter() -> DuckDBAdapter:
    db = DuckDBAdapter(":memory:")
    db.connection.execute(
        """
        CREATE SCHEMA IF NOT EXISTS analytics;
        CREATE TABLE analytics.orders (
            id INTEGER, amount DECIMAL(10,2), tenant_id VARCHAR
        );
        INSERT INTO analytics.orders VALUES (1, 100.00, 'acme');
        CREATE TABLE analytics.customers (id INTEGER, name VARCHAR, tenant_id VARCHAR);
        CREATE TABLE analytics.subscriptions (
            id INTEGER, plan VARCHAR, tenant_id VARCHAR
        );
        """
    )
    return db


@pytest.fixture
def semantic(fixtures_dir: Path) -> YamlSource:
    return YamlSource(fixtures_dir / "semantic_source.yml")


@pytest.mark.asyncio
async def test_inspect_query_valid_with_adapter(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "inspect_query")
    result = await tool.callable(
        {"sql": "SELECT id, amount FROM analytics.orders WHERE tenant_id = 'acme'"}
    )
    data = json.loads(result["content"][0]["text"])
    assert data["valid"] is True
    assert data["violations"] == []
    assert data["schema_valid"] is True


@pytest.mark.asyncio
async def test_inspect_query_blocked_surfaces_violations(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "inspect_query")
    result = await tool.callable({"sql": "SELECT * FROM analytics.orders"})
    data = json.loads(result["content"][0]["text"])
    assert data["valid"] is False
    assert len(data["violations"]) >= 1


@pytest.mark.asyncio
async def test_inspect_query_no_adapter(
    contract: DataContract, semantic: YamlSource
) -> None:
    tools = create_tools(contract, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "inspect_query")
    result = await tool.callable(
        {"sql": "SELECT id FROM analytics.orders WHERE tenant_id = 'acme'"}
    )
    data = json.loads(result["content"][0]["text"])
    # Layer 1 still runs; EXPLAIN fields are absent or null
    assert "valid" in data
    assert data.get("estimated_cost_usd") is None
    assert data.get("estimated_rows") is None


@pytest.mark.asyncio
async def test_inspect_query_returns_pending_result_checks(
    adapter: DuckDBAdapter,
) -> None:
    from agentic_data_contracts.core.schema import (
        AllowedTable,
        DataContractSchema,
        Enforcement,
        ResultCheck,
        SemanticConfig,
        SemanticRule,
    )

    dc = DataContract(
        DataContractSchema(
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
    )
    tools = create_tools(dc, adapter=adapter)
    tool = next(t for t in tools if t.name == "inspect_query")
    result = await tool.callable(
        {"sql": "SELECT id, amount FROM analytics.orders"}
    )
    data = json.loads(result["content"][0]["text"])
    assert "no_negative" in data["pending_result_checks"]
```

- [ ] **Step 2: Run the tests — confirm they fail with "inspect_query not found"**

Run: `uv run pytest tests/test_tools/test_inspect_query.py -v`
Expected: all four tests fail with `StopIteration` from `next(t for t in tools if t.name == "inspect_query")`.

- [ ] **Step 3: Implement `inspect_query` in `factory.py`**

Add this function inside `create_tools()`, just before the `# ── Tool 12: get_contract_info ──` comment (around line 657). It can go anywhere among the inner async functions, but placing it near `run_query` keeps the query-lifecycle tools grouped.

```python
# ── Tool: inspect_query ───────────────────────────────────────────────────
async def inspect_query(args: dict[str, Any]) -> dict[str, Any]:
    sql = args.get("sql", "")
    result = validator.validate(sql)
    data: dict[str, Any] = {
        "valid": not result.blocked,
        "violations": list(result.reasons),
        "warnings": list(result.warnings),
        "schema_valid": result.schema_valid,
        "pending_result_checks": list(validator.pending_result_check_names()),
    }
    if result.estimated_cost_usd is not None:
        data["estimated_cost_usd"] = result.estimated_cost_usd
    if result.estimated_rows is not None:
        data["estimated_rows"] = result.estimated_rows
    if result.explain_errors:
        data["explain_errors"] = list(result.explain_errors)
    return _text_response(json.dumps(data, default=str))
```

Then register it in the returned `ToolDef` list at the bottom of `create_tools()`. Add this entry just before the `validate_query` `ToolDef` (we will remove `validate_query` and `query_cost_estimate` in a later task):

```python
ToolDef(
    name="inspect_query",
    description=(
        "Inspect a SQL query without executing it. Returns validation result"
        " (violations and warnings from contract rules), estimated cost and row"
        " count from EXPLAIN when a database adapter is configured, schema"
        " validity, and any result checks that would run after execution."
        " Use this to iterate on SQL before spending retry budget on run_query."
    ),
    input_schema={
        "type": "object",
        "properties": {
            "sql": {"type": "string", "description": "SQL query to inspect"}
        },
        "required": ["sql"],
    },
    callable=inspect_query,
),
```

- [ ] **Step 4: Run the new tests — confirm they pass**

Run: `uv run pytest tests/test_tools/test_inspect_query.py -v`
Expected: all four pass.

- [ ] **Step 5: Run the full tools suite — confirm nothing else regressed**

Run: `uv run pytest tests/test_tools -v`
Expected: `test_factory.py` still passes; the count assertion currently reads `== 13` but since we ADDED a tool we now have 14 — and the count test will fail. **That is expected at this stage** and will be fixed in Task 4. The other tests should be green.

- [ ] **Step 6: Commit**

```bash
git add src/agentic_data_contracts/tools/factory.py tests/test_tools/test_inspect_query.py
git commit -m "feat(tools): add inspect_query tool merging validate_query + query_cost_estimate"
```

---

## Task 3: Extend `run_query` response with session budget (TDD)

**Rationale:** After dropping `get_contract_info`, agents still need to know remaining budget. Surfacing `session.remaining()` on every `run_query` response removes that tool-selection decision.

**Files:**
- Modify: `src/agentic_data_contracts/tools/factory.py:593-655` (the `run_query` function)
- Modify: `tests/test_tools/test_factory.py` (add test; do not yet touch the count-assertion tests)

- [ ] **Step 1: Add failing tests to `test_factory.py`**

Append to `tests/test_tools/test_factory.py`:

```python
@pytest.mark.asyncio
async def test_run_query_response_includes_session_remaining(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "run_query")
    result = await tool.callable(
        {"sql": "SELECT id FROM analytics.orders WHERE tenant_id = 'acme'"}
    )
    data = json.loads(result["content"][0]["text"])
    assert "session" in data
    assert "remaining" in data["session"]
    assert "elapsed_seconds" in data["session"]["remaining"]


@pytest.mark.asyncio
async def test_run_query_blocked_includes_remaining_budget(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "run_query")
    result = await tool.callable({"sql": "DELETE FROM analytics.orders"})
    text = result["content"][0]["text"]
    assert "block" in text.lower() or "violation" in text.lower()
    assert "remaining" in text.lower()
```

- [ ] **Step 2: Run the two new tests — confirm they fail**

Run: `uv run pytest tests/test_tools/test_factory.py::test_run_query_response_includes_session_remaining tests/test_tools/test_factory.py::test_run_query_blocked_includes_remaining_budget -v`
Expected: FAIL — the success response has no `session` block and the blocked response has no "remaining" text.

- [ ] **Step 3: Extend `run_query` on the success path**

In `factory.py`, locate the success branch inside `run_query` (currently lines 641–655). Replace the block that builds `data` and `response_text` with:

```python
rows = [dict(zip(qresult.columns, row)) for row in qresult.rows]
data = {
    "columns": qresult.columns,
    "rows": rows,
    "row_count": qresult.row_count,
    "session": {"remaining": session.remaining()},
}
response_text = json.dumps(data, default=str)

# Prepend warnings from both query checks and result checks
all_warnings = vresult.warnings + rresult.warnings
if all_warnings:
    warning_text = "WARNINGS:\n" + "\n".join(f"- {w}" for w in all_warnings)
    response_text = warning_text + "\n\n" + response_text

return _text_response(response_text)
```

- [ ] **Step 4: Extend `run_query` on each blocked path**

Locate the four blocked return sites inside `run_query`:
1. Session limit exceeded (`LimitExceededError`)
2. Validator blocked pre-execution
3. Adapter execute raised an exception
4. Result checks blocked

For each, immediately before the `return _text_response(msg)` (or equivalent one-line return), append remaining-budget context. Use this helper pattern — add the helper just below the `run_query` definition and call it from each site:

```python
def _with_remaining(msg: str) -> str:
    return f"{msg}\nRemaining: {json.dumps(session.remaining(), default=str)}"
```

Then the four sites become:
- Session limit: `return _text_response(_with_remaining(f"BLOCKED — Session limit exceeded: {e}"))`
- Validator blocked: `return _text_response(_with_remaining(msg))`
- Execute exception: `return _text_response(_with_remaining(f"BLOCKED — Query execution failed: {e}"))`
- Result checks blocked: `return _text_response(_with_remaining(msg))`

Define `_with_remaining` as a regular (non-async) nested function inside `run_query` or inside `create_tools` at the outer scope — whichever closes over `session`. The simpler choice is a nested function inside `run_query`, placed after the `sql = args.get("sql", "")` line.

- [ ] **Step 5: Run the two new tests — confirm they pass**

Run: `uv run pytest tests/test_tools/test_factory.py::test_run_query_response_includes_session_remaining tests/test_tools/test_factory.py::test_run_query_blocked_includes_remaining_budget -v`
Expected: PASS.

- [ ] **Step 6: Run the full tools suite — catch collateral breakage**

Run: `uv run pytest tests/test_tools -v`
Expected: existing `test_run_query_*` tests still pass (they only check for substrings like "100" or "block" / "violation", which remain present). The tool count test still fails — addressed in Task 4.

- [ ] **Step 7: Commit**

```bash
git add src/agentic_data_contracts/tools/factory.py tests/test_tools/test_factory.py
git commit -m "feat(tools): include session remaining budget in run_query responses"
```

---

## Task 4: Update test files to expect the new tool surface

**Rationale:** Before we remove tools from `factory.py`, update tests to expect the 9-tool set. This makes the failures after Task 5 informative (green) rather than noisy.

**Files:**
- Modify: `tests/test_tools/test_factory.py`
- Delete: `tests/test_tools/test_pagination.py`
- Modify: `tests/test_tools/test_wildcard_tools.py`
- Modify: `tests/test_tools/test_semantic_tools.py`

- [ ] **Step 1: Update `test_factory.py` count and names**

In `tests/test_tools/test_factory.py`:

Rename `test_create_tools_returns_13_tools` to `test_create_tools_returns_9_tools` and change the assertion from `13` to `9`. Do the same inside `test_create_tools_without_adapter` (also asserts 13 currently).

Replace the body of `test_tool_names` with:

```python
def test_tool_names(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    names = {t.name for t in tools}
    expected = {
        "describe_table",
        "preview_table",
        "list_metrics",
        "lookup_metric",
        "lookup_domain",
        "lookup_relationships",
        "trace_metric_impacts",
        "inspect_query",
        "run_query",
    }
    assert names == expected
```

- [ ] **Step 2: Delete obsolete tests from `test_factory.py`**

Delete the following test functions entirely:
- `test_list_schemas` (around line 73)
- `test_list_tables` (around line 88)
- `test_validate_query_passes` (around line 123)
- `test_validate_query_blocked` (around line 136)
- `test_get_contract_info` (around line 171)
- `test_query_cost_estimate_with_adapter` (around line 244)
- `test_query_cost_estimate_without_adapter` (around line 255)

Keep everything else, including the new `test_run_query_response_includes_session_remaining` and `test_run_query_blocked_includes_remaining_budget` added in Task 3.

- [ ] **Step 3: Delete `test_pagination.py`**

Run: `git rm tests/test_tools/test_pagination.py`

Rationale: the entire file tests the `list_tables` tool, which is dropped. Pagination logic itself is removed with the tool.

- [ ] **Step 4: Prune `test_wildcard_tools.py`**

In `tests/test_tools/test_wildcard_tools.py`:

Delete `test_list_tables_after_wildcard_resolve` (around line 42) entirely.

Rewrite `test_validate_query_with_wildcard_tables` (around line 67) to use `inspect_query` instead. Replace the function body with:

```python
@pytest.mark.asyncio
async def test_inspect_query_with_wildcard_tables(
    wildcard_contract: DataContract, adapter: DuckDBAdapter
) -> None:
    tools = create_tools(wildcard_contract, adapter=adapter)
    tool = next(t for t in tools if t.name == "inspect_query")
    # analytics.orders should be allowed after wildcard resolution
    result = await tool.callable({"sql": "SELECT id FROM analytics.orders"})
    data = json.loads(result["content"][0]["text"])
    assert data["valid"] is True
```

Also rename the function to `test_inspect_query_with_wildcard_tables` (as shown above).

- [ ] **Step 5: Prune `test_semantic_tools.py`**

In `tests/test_tools/test_semantic_tools.py`:

Delete `test_get_contract_info_includes_domains` (around line 192) entirely.
Delete `test_list_schemas_with_description_and_preferred` (around line 272) entirely — this test's coverage (description + preferred flags in schema entries) now lives entirely in the prompt renderer's output, which has its own tests under `tests/test_core/test_prompt.py` (if present) or is covered implicitly.

- [ ] **Step 6: Run the tools suite — confirm expected failure mode**

Run: `uv run pytest tests/test_tools -v`
Expected: `test_create_tools_returns_9_tools` and `test_tool_names` FAIL with "assert 14 == 9" and the expected-set mismatch. All other tests PASS.

(This is the pre-deletion red state. Task 5 drops the tools to turn it green.)

- [ ] **Step 7: Commit**

```bash
git add tests/test_tools/
git commit -m "test(tools): update expectations for 9-tool surface"
```

---

## Task 5: Remove the 5 dropped tools from `factory.py`

**Rationale:** With tests updated, remove the five redundant tool functions and their `ToolDef` registrations.

**Files:**
- Modify: `src/agentic_data_contracts/tools/factory.py`

- [ ] **Step 1: Delete tool function definitions**

In `src/agentic_data_contracts/tools/factory.py`, delete these async function definitions entirely (including their `# ── Tool N: ... ──` comment headers):

- `list_schemas` (lines 201–213)
- `list_tables` (lines 215–251)
- `validate_query` (lines 552–573)
- `query_cost_estimate` (lines 575–591)
- `get_contract_info` (lines 657–701)

- [ ] **Step 2: Delete the corresponding `ToolDef` entries from the returned list**

In the `return [ ... ]` block at the bottom of `create_tools()`, delete these entries:

- `name="list_schemas"` (lines 705–713)
- `name="list_tables"` (lines 714–740)
- `name="validate_query"` (lines 849–863)
- `name="query_cost_estimate"` (lines 864–877)
- `name="get_contract_info"` (lines 954–962)

- [ ] **Step 3: Update the module-level and `create_tools` docstrings**

At `factory.py:1`, change the module docstring from:

```python
"""Tool factory — creates 13 agent tools from a DataContract."""
```

to:

```python
"""Tool factory — creates 9 agent tools from a DataContract."""
```

Renumber the remaining `# ── Tool N: ... ──` comment headers so they run 1 through 9 in the order they appear in the function. (This is cosmetic; if the comments have drifted, use sensible numbering or drop the numbers entirely.)

- [ ] **Step 4: Run the tools suite — confirm everything is green**

Run: `uv run pytest tests/test_tools -v`
Expected: all tests pass.

- [ ] **Step 5: Run the full test suite**

Run: `uv run pytest -v`
Expected: all tests pass across `test_core`, `test_validation`, `test_semantic`, `test_adapters`, `test_bridge`, `test_tools`, `test_public_api.py`.

- [ ] **Step 6: Commit**

```bash
git add src/agentic_data_contracts/tools/factory.py
git commit -m "feat(tools)!: drop list_schemas, list_tables, get_contract_info, validate_query, query_cost_estimate"
```

---

## Task 6: Update the `sdk.py` docstring

**Files:**
- Modify: `src/agentic_data_contracts/tools/sdk.py:27`

- [ ] **Step 1: Fix the tool count in the docstring**

In `src/agentic_data_contracts/tools/sdk.py`, find this line in the `create_sdk_mcp_server` docstring (around line 27):

```
    Wraps all 13 contract tools with the SDK's @tool decorator and
```

Change `13` to `9`.

- [ ] **Step 2: Run the SDK tests**

Run: `uv run pytest tests/test_tools/test_sdk.py -v`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add src/agentic_data_contracts/tools/sdk.py
git commit -m "docs(sdk): update tool count in create_sdk_mcp_server docstring"
```

---

## Task 7: Update `README.md`

**Files:**
- Modify: `README.md` (lines with dropped tool references)

- [ ] **Step 1: Update the quickstart narrative at `README.md:25`**

Line 25 currently reads:

```
5. validate_query(sql)          → VALID (passes all contract rules)
```

Replace with:

```
5. inspect_query(sql)           → {"valid": true, "estimated_cost_usd": 0.0, ...}
```

Adjust numbering of surrounding steps if the removal shifts them.

- [ ] **Step 2: Update the "wraps all 13 tools" comment at `README.md:136`**

Change `wraps all 13 tools` to `wraps all 9 tools`.

- [ ] **Step 3: Update the validate snippet at `README.md:163`**

The existing line is:

```python
    validate = next(t for t in tools if t.name == "validate_query")
```

Change to:

```python
    inspect = next(t for t in tools if t.name == "inspect_query")
```

Update the surrounding code and variable uses accordingly (the call site likely uses `validate(...)` — rename to `inspect(...)`).

- [ ] **Step 4: Update the tool table (lines 183–195)**

Remove these rows:
- `| `list_schemas` | ... |`
- `| `list_tables` | ... |`
- `| `validate_query` | ... |`
- `| `query_cost_estimate` | ... |`
- `| `get_contract_info` | ... |`

Add this row (place it near the end, alongside `run_query`):

```
| `inspect_query` | Validate a SQL query and estimate its cost via EXPLAIN without executing |
```

- [ ] **Step 5: Remove the pagination reference at `README.md:480`**

Delete the entire line that mentions `list_tables` pagination, or reframe that row if it's part of a multi-column table. If the surrounding section documents table discovery, replace the row with one that points at `describe_table` for on-demand column details.

- [ ] **Step 6: Scan for any remaining stale references**

Run: `grep -n "list_schemas\|list_tables\|get_contract_info\|validate_query\|query_cost_estimate\|13 tool" README.md`
Expected: no output, except any intentional historical mention inside changelog-style sections (none exist in this README).

- [ ] **Step 7: Commit**

```bash
git add README.md
git commit -m "docs(readme): update tool surface references to 9-tool set"
```

---

## Task 8: Update `docs/architecture.md`

**Files:**
- Modify: `docs/architecture.md`

- [ ] **Step 1: Replace the numbered tool list (lines 308–326)**

The current list has 13 numbered entries. Replace the block (everything from `1. **`list_schemas()`**` through `13. **`get_contract_info()`**`) with:

```
1. **`describe_table(schema, table)`** — Column details from the database adapter
2. **`preview_table(schema, table, limit?)`** — Sample rows
3. **`list_metrics(domain?, tier?, indicator_kind?)`** — Browse metrics with filters
4. **`lookup_metric(metric_name)`** — Full metric definition with SQL and impact edges
5. **`lookup_domain(name)`** — Full domain description with metrics and tables
6. **`lookup_relationships(table, target_table?)`** — Direct joins and multi-hop paths
7. **`trace_metric_impacts(metric_name, direction, max_depth?)`** — BFS over the impact graph
8. **`inspect_query(sql)`** — Static + EXPLAIN check, no execution
9. **`run_query(sql)`** — Validate and execute; response includes remaining session budget
```

- [ ] **Step 2: Update the workflow diagram (lines 331–338)**

Replace the example flow with one that uses only surviving tools:

```
list_metrics → lookup_metric → lookup_relationships → describe_table
    → write SQL → inspect_query
    → (if valid) run_query
```

Remove the line that references `get_contract_info`.

- [ ] **Step 3: Update the "all 13 tools" comment at line 350**

Change `Returns all 13 tools` to `Returns all 9 tools`.

- [ ] **Step 4: Update the tool-coverage table (lines 390–393)**

Replace the three rows with:

```
| `describe_table`, `preview_table`, `list_metrics`, `lookup_metric`, `lookup_domain`, `lookup_relationships`, `trace_metric_impacts` | Fully functional (contract + semantic source) |
| `run_query` | Fully functional when database adapter is configured |
| `inspect_query` | Layer 1 always runs; EXPLAIN fields populated when adapter is configured |
```

- [ ] **Step 5: Update the "returns 13 tools" comment at line 531**

Change `# create_tools() — returns 13 tools` to `# create_tools() — returns 9 tools`.

- [ ] **Step 6: Scan for any remaining stale references**

Run: `grep -n "list_schemas\|list_tables\|get_contract_info\|validate_query\|query_cost_estimate\|13 tool" docs/architecture.md`
Expected: no output.

- [ ] **Step 7: Commit**

```bash
git add docs/architecture.md
git commit -m "docs(architecture): update tool list and workflow for 9-tool surface"
```

---

## Task 9: Version bump + CHANGELOG entry

**Files:**
- Modify: `pyproject.toml:3`
- Modify: `CHANGELOG.md` (prepend new entry)

- [ ] **Step 1: Bump version in `pyproject.toml`**

Change `pyproject.toml:3` from:

```
version = "0.10.0"
```

to:

```
version = "0.11.0"
```

- [ ] **Step 2: Prepend a `0.11.0` entry to `CHANGELOG.md`**

At the top of `CHANGELOG.md`, just above the `## [0.10.0]` heading, insert:

```
## [0.11.0] - 2026-04-17

### Breaking

- **Tool surface consolidated from 13 to 9 tools**: Five tools dropped and two merged into one. The full contract is already injected into the system prompt by `ClaudePromptRenderer`, so the dropped tools were redundant from an analytics-agent perspective.
- **`list_schemas` removed**: The allowed-schemas set is implicit in the allowed-tables list that the prompt renderer already injects.
- **`list_tables` removed**: The prompt renderer already injects the full allowed-tables list. Per-table column details remain available via `describe_table`.
- **`get_contract_info` removed**: Contract name, allowed tables, rules, and limits are all in the prompt. The one dynamic field the tool exposed — remaining session budget — is now embedded in every `run_query` response under `session.remaining`.
- **`validate_query` + `query_cost_estimate` merged into `inspect_query`**: Both tools wrapped the same underlying `Validator.validate()` call (which internally runs Layer 1 + EXPLAIN). The merge removes a "which tool do I call?" decision. Response is structured JSON with `valid`, `violations`, `warnings`, `schema_valid`, `pending_result_checks`, and — when an adapter is configured — `estimated_cost_usd`, `estimated_rows`, and any `explain_errors`.

### Changed

- **`run_query` response**: Success responses now include a `session.remaining` block mirroring `ContractSession.remaining()` (elapsed seconds, retries remaining, token budget remaining, cost remaining). Blocked responses append a one-line `Remaining: {...}` suffix with the same data.
- **`ValidationResult` dataclass**: Gains three additive fields — `estimated_rows: int | None`, `schema_valid: bool = True`, and `explain_errors: list[str] = []`. Populated in `Validator.validate()` when an `ExplainAdapter` is configured. Defaults are safe for existing callers.

### Migration

- Replace `validate_query(sql)` calls with `inspect_query(sql)`. The response is JSON rather than a status string; parse `valid`, `violations`, and `warnings`. Cost and row estimates live under the same response.
- Replace `query_cost_estimate(sql)` calls with `inspect_query(sql)`. Cost and row fields are now nested alongside validation fields.
- If an agent previously called `get_contract_info`, read remaining budget from `run_query` responses (`data["session"]["remaining"]`) instead. Static contract metadata is already in the system prompt.
- `list_schemas` and `list_tables` have no replacements — the prompt already contains this information.
```

- [ ] **Step 3: Verify CHANGELOG format matches existing entries**

Eyeball the new entry against the `[0.10.0]` entry directly below it. Both should use the same heading levels and section names (`### Breaking`, `### Changed`, etc.). Fix any formatting drift.

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml CHANGELOG.md
git commit -m "chore(release): bump to 0.11.0 for tool surface consolidation"
```

---

## Task 10: Final verification

**Files:** none (verification only)

- [ ] **Step 1: Run the full test suite**

Run: `uv run pytest -v`
Expected: all tests pass.

- [ ] **Step 2: Run lint and format checks**

Run: `uv run ruff check src/ tests/`
Expected: "All checks passed!"

Run: `uv run ruff format --check src/ tests/`
Expected: "X files already formatted"

- [ ] **Step 3: Run the type checker**

Run: `ty check`
Expected: no errors.

- [ ] **Step 4: Run pre-commit hooks**

Run: `prek run --all-files`
Expected: all hooks pass.

- [ ] **Step 5: Verify the final tool count at runtime**

Run:
```bash
uv run python -c "
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.tools.factory import create_tools
dc = DataContract.from_yaml('tests/fixtures/valid_contract.yml')
tools = create_tools(dc)
print(f'tool count: {len(tools)}')
for t in tools:
    print(f'  - {t.name}')
"
```
Expected: tool count: 9, followed by the nine tool names.

- [ ] **Step 6: Push the branch (if working on a branch)**

Run: `git push origin HEAD`

---

## Self-review Notes

**Spec coverage check:** Every section of the spec maps to a task:
- Dropped tools (5) → Task 5
- `inspect_query` new tool → Task 2 (with Task 1 as prerequisite for EXPLAIN fields)
- `run_query` session budget → Task 3
- Clean break, version bump, CHANGELOG → Task 9
- `sdk.py` docstring → Task 6
- Test removals → Task 4
- README updates → Task 7
- Architecture doc → Task 8
- Final verification → Task 10

**Type consistency:** `inspect_query` response keys (`valid`, `violations`, `warnings`, `schema_valid`, `pending_result_checks`, `estimated_cost_usd`, `estimated_rows`, `explain_errors`) match the spec. `ValidationResult` field names (`estimated_rows`, `schema_valid`, `explain_errors`) flow consistently from Task 1 through Task 2.

**No placeholders:** every code block shows the concrete change; every command has expected output.
