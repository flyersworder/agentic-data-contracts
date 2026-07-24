# Compact Row Encoding Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the dict-per-row JSON rendering in `run_query` and `preview_table` with positional arrays aligned to a `columns` key, behind a `create_tools(row_format=...)` operator knob that defaults to the compact rendering.

**Architecture:** A single pure helper `_render_rows` in `tools/factory.py` owns the branch; both tools call it. `create_tools` gains a keyword-only `row_format` parameter validated eagerly at call time, and the four ecosystem wrappers accept and forward it. `preview_table` additionally gains a `columns` key in both modes so the two tools share one envelope shape.

**Tech Stack:** Python 3.11+, `uv`, pytest (+ `pytest-asyncio`), sqlglot, DuckDB (test adapter), ruff + ty via prek.

**Spec:** `docs/superpowers/specs/2026-07-24-compact-row-encoding-design.md`
**Issue:** [#44](https://github.com/flyersworder/agentic-data-contracts/issues/44)

## Global Constraints

- Run everything Python-related through `uv run`. Never invoke a bare `pytest`/`python`.
- Run linters through prek, never directly: `prek run --all-files` is what CI runs.
- Follow TDD: the failing test is written and *observed failing* before implementation.
- The `records` rendering must reproduce today's output byte-for-byte. `json.dumps(..., default=str)` stays in place unchanged at every call site.
- No new dependencies.
- Branch is `feat/compact-row-encoding`, already created. Do not merge or push; stop after the final task.
- Valid `row_format` values are exactly `"compact"` and `"records"`. Default is `"compact"` everywhere it appears.
- The description clause, used verbatim wherever it appears (note the leading space):
  `" Rows are arrays of values positionally aligned to \`columns\`."`

---

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `src/agentic_data_contracts/tools/factory.py` | `RowFormat` type, `_ROW_FORMATS`, `_COMPACT_ROWS_NOTE`, `_render_rows`, `create_tools` parameter + validation, both tool call sites, both descriptions | 1, 2, 3 |
| `src/agentic_data_contracts/__init__.py` | Re-export `RowFormat` | 1 |
| `src/agentic_data_contracts/tools/langchain.py` | Accept + forward `row_format` | 4 |
| `src/agentic_data_contracts/tools/sdk.py` | Accept + forward `row_format` | 4 |
| `src/agentic_data_contracts/tools/pydantic_ai.py` | Accept + forward `row_format` in both public functions; eager validation in the toolset | 4 |
| `tests/test_tools/test_row_format.py` | New — all tests for this feature | 1, 2, 3, 4 |
| `tests/test_tools/test_factory_principals.py:128` | Update the one assertion that reads a row as a dict | 3 |
| `README.md`, `docs/architecture.md`, `CHANGELOG.md`, `pyproject.toml` | Docs + release | 5 |

---

### Task 1: `RowFormat` type and the `_render_rows` helper

Pure, no I/O, no database. Establishes the names every later task consumes.

**Files:**
- Modify: `src/agentic_data_contracts/tools/factory.py` (imports at top; new definitions after `_text_response`, ~line 50)
- Modify: `src/agentic_data_contracts/__init__.py:22` and `:63`
- Test: `tests/test_tools/test_row_format.py` (create)

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `RowFormat = Literal["compact", "records"]` in `tools/factory.py`, re-exported from `agentic_data_contracts`
  - `_ROW_FORMATS: tuple[RowFormat, ...] = ("compact", "records")`
  - `_render_rows(columns: Sequence[str], rows: Iterable[Sequence[Any]], row_format: RowFormat) -> list[Any]`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_tools/test_row_format.py`:

```python
"""Row-encoding tests for run_query / preview_table (issue #44)."""

import json
from datetime import date
from decimal import Decimal

from agentic_data_contracts.tools.factory import _render_rows

COLUMNS = ["region", "units", "note"]
ROWS = [
    ("EMEA", 412, None),
    ("APAC", 87, "re-run\tpending\nline2"),
    ("AMER", 0, ""),
]


class _DriverRow:
    """A row that is iterable and indexable but is neither list nor tuple.

    Stands in for a third-party adapter returning its driver's row type.
    ``dict(zip(...))`` accepts this; ``json.dumps`` does not.
    """

    def __init__(self, *values: object) -> None:
        self._values = list(values)

    def __iter__(self):
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def __getitem__(self, index: int) -> object:
        return self._values[index]


def test_compact_renders_positional_arrays() -> None:
    assert _render_rows(COLUMNS, ROWS, "compact") == [
        ["EMEA", 412, None],
        ["APAC", 87, "re-run\tpending\nline2"],
        ["AMER", 0, ""],
    ]


def test_records_renders_one_dict_per_row() -> None:
    assert _render_rows(COLUMNS, ROWS, "records")[0] == {
        "region": "EMEA",
        "units": 412,
        "note": None,
    }


def test_empty_rows_render_empty_in_both_modes() -> None:
    assert _render_rows(COLUMNS, [], "compact") == []
    assert _render_rows(COLUMNS, [], "records") == []


def test_null_stays_distinct_from_empty_string() -> None:
    rendered = json.loads(
        json.dumps(_render_rows(COLUMNS, ROWS, "compact"), default=str)
    )
    assert rendered[0][2] is None
    assert rendered[2][2] == ""


def test_tab_and_newline_survive_serialization() -> None:
    rendered = json.loads(
        json.dumps(_render_rows(COLUMNS, ROWS, "compact"), default=str)
    )
    assert rendered[1][2] == "re-run\tpending\nline2"


def test_decimal_and_date_coerce_identically_in_both_modes() -> None:
    columns = ["salary", "hired"]
    rows = [(Decimal("100000.00"), date(2025, 1, 31))]
    compact = json.loads(
        json.dumps(_render_rows(columns, rows, "compact"), default=str)
    )
    records = json.loads(
        json.dumps(_render_rows(columns, rows, "records"), default=str)
    )
    assert compact[0] == ["100000.00", "2025-01-31"]
    assert records[0] == {"salary": "100000.00", "hired": "2025-01-31"}


def test_non_tuple_row_serializes_as_array_not_string() -> None:
    # Without the list(row) coercion json.dumps routes _DriverRow through
    # default=str and emits "<_DriverRow object at 0x...>" instead of an array.
    rendered = json.loads(
        json.dumps(
            _render_rows(COLUMNS, [_DriverRow("EMEA", 412, None)], "compact"),
            default=str,
        )
    )
    assert rendered == [["EMEA", 412, None]]


def test_row_format_is_exported_from_package_root() -> None:
    import agentic_data_contracts

    assert "RowFormat" in agentic_data_contracts.__all__
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_tools/test_row_format.py -v`
Expected: collection error — `ImportError: cannot import name '_render_rows' from 'agentic_data_contracts.tools.factory'`

- [ ] **Step 3: Add the imports**

In `src/agentic_data_contracts/tools/factory.py`, replace:

```python
from dataclasses import dataclass
from datetime import date
from typing import Any
```

with:

```python
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any, Literal
```

- [ ] **Step 4: Add the type, constants, and helper**

In the same file, immediately after `_text_response` (which ends at ~line 50 with `return {"content": [{"type": "text", "text": text}]}`), insert:

```python
RowFormat = Literal["compact", "records"]

_ROW_FORMATS: tuple[RowFormat, ...] = ("compact", "records")

_COMPACT_ROWS_NOTE = " Rows are arrays of values positionally aligned to `columns`."


def _render_rows(
    columns: Sequence[str],
    rows: Iterable[Sequence[Any]],
    row_format: RowFormat,
) -> list[Any]:
    """Render result rows for JSON serialization.

    ``compact`` emits positional arrays aligned to ``columns``; ``records``
    emits one dict per row (the pre-0.31 rendering).

    The ``list(row)`` coercion is load-bearing. ``QueryResult.rows`` is
    annotated ``list[tuple[Any, ...]]``, but ``DatabaseAdapter`` is a
    ``@runtime_checkable`` Protocol, so a third-party adapter may hand back its
    driver's row type. ``zip`` only needs iteration, so the ``records`` branch
    tolerates that; ``json.dumps`` does not — a row that is neither list nor
    tuple falls through to ``default=str`` and serializes as a *string*.
    """
    if row_format == "compact":
        return [list(row) for row in rows]
    return [dict(zip(columns, row)) for row in rows]
```

- [ ] **Step 5: Re-export `RowFormat`**

In `src/agentic_data_contracts/__init__.py`, change line 22 from:

```python
from agentic_data_contracts.tools.factory import create_tools
```

to:

```python
from agentic_data_contracts.tools.factory import RowFormat, create_tools
```

and insert `"RowFormat",` into `__all__` between `"Relationship",` (line 62) and `"SemanticSource",` (line 63), preserving alphabetical order.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `uv run pytest tests/test_tools/test_row_format.py -v`
Expected: 9 passed

- [ ] **Step 7: Run the full suite and linters**

Run: `uv run pytest -q && prek run --all-files`
Expected: full suite green (785 tests + 9 new), all hooks pass

- [ ] **Step 8: Commit**

```bash
git add src/agentic_data_contracts/tools/factory.py \
        src/agentic_data_contracts/__init__.py \
        tests/test_tools/test_row_format.py
git commit -m "feat: add _render_rows helper and RowFormat type

Pure rendering helper for result rows, positional arrays or one dict
per row. Not yet wired into any tool. Refs #44"
```

---

### Task 2: `create_tools(row_format=...)` and `run_query`

Adds the parameter with eager validation and switches `run_query` to the helper. `preview_table` is deliberately left alone until Task 3.

**Files:**
- Modify: `src/agentic_data_contracts/tools/factory.py:148-158` (signature + validation), `:788-793` (rows), `:947-948` (description)
- Test: `tests/test_tools/test_row_format.py` (append)

**Interfaces:**
- Consumes: `_render_rows`, `RowFormat`, `_ROW_FORMATS`, `_COMPACT_ROWS_NOTE` from Task 1.
- Produces: `create_tools(..., row_format: RowFormat = "compact")`; a `rows_note` local in `create_tools` that Task 3 also uses.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_tools/test_row_format.py`. Add these imports at the top of the file, below the existing ones:

```python
from pathlib import Path

import pytest

from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.semantic.yaml_source import YamlSource
from agentic_data_contracts.tools.factory import create_tools
```

Then append:

```python
SQL = "SELECT id, amount FROM analytics.orders WHERE tenant_id = 'acme'"


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
        INSERT INTO analytics.orders VALUES (1, 100.00, 'acme'), (2, 200.00, 'acme');
        CREATE TABLE analytics.customers (id INTEGER, name VARCHAR, tenant_id VARCHAR);
        CREATE TABLE analytics.subscriptions (
            id INTEGER, plan VARCHAR, tenant_id VARCHAR
        );
        """
    )
    return db
```

(The two extra tables mirror `tests/test_tools/test_factory.py`'s `adapter`
fixture exactly. They are unused by these tests, but `create_tools` resolves
wildcard tables against the adapter when the contract declares any, so the
fixture must present the same schema the rest of the suite assumes.)

```python


@pytest.fixture
def semantic(fixtures_dir: Path) -> YamlSource:
    return YamlSource(fixtures_dir / "semantic_source.yml")


def _tool(tools: list, name: str):
    return next(t for t in tools if t.name == name)


def test_unknown_row_format_raises_at_create_time(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    with pytest.raises(ValueError, match="row_format must be one of"):
        create_tools(contract, adapter=adapter, row_format="bogus")  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_run_query_compact_is_the_default(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    result = await _tool(tools, "run_query").callable({"sql": SQL})
    body = json.loads(result["content"][0]["text"])
    assert body["columns"] == ["id", "amount"]
    assert body["rows"] == [[1, "100.00"], [2, "200.00"]]


@pytest.mark.asyncio
async def test_run_query_records_reproduces_legacy_shape(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(
        contract, adapter=adapter, semantic_source=semantic, row_format="records"
    )
    result = await _tool(tools, "run_query").callable({"sql": SQL})
    body = json.loads(result["content"][0]["text"])
    assert body["rows"] == [
        {"id": 1, "amount": "100.00"},
        {"id": 2, "amount": "200.00"},
    ]


@pytest.mark.asyncio
async def test_both_modes_agree_column_for_column(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    compact = json.loads(
        (
            await _tool(
                create_tools(contract, adapter=adapter, semantic_source=semantic),
                "run_query",
            ).callable({"sql": SQL})
        )["content"][0]["text"]
    )
    records = json.loads(
        (
            await _tool(
                create_tools(
                    contract,
                    adapter=adapter,
                    semantic_source=semantic,
                    row_format="records",
                ),
                "run_query",
            ).callable({"sql": SQL})
        )["content"][0]["text"]
    )
    rebuilt = [dict(zip(compact["columns"], row)) for row in compact["rows"]]
    assert rebuilt == records["rows"]


def test_run_query_description_documents_compact_shape(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    compact = _tool(create_tools(contract, adapter=adapter), "run_query")
    records = _tool(
        create_tools(contract, adapter=adapter, row_format="records"), "run_query"
    )
    assert "positionally aligned" in compact.description
    assert records.description == (
        "Validate and execute a SQL query, returning the results."
    )
```

- [ ] **Step 2: Run the new tests to verify they fail**

Run: `uv run pytest tests/test_tools/test_row_format.py -v -k "row_format or run_query or agree"`
Expected: FAIL — `create_tools` raises `TypeError: create_tools() got an unexpected keyword argument 'row_format'`

- [ ] **Step 3: Add the parameter and eager validation**

In `src/agentic_data_contracts/tools/factory.py`, change the `create_tools` signature (line 148) from:

```python
def create_tools(
    contract: DataContract,
    *,
    adapter: DatabaseAdapter | None = None,
    semantic_source: SemanticSource | None = None,
    session: ContractSession | None = None,
    caller_principal: Principal = None,
    staleness_threshold_days: int = 90,
) -> list[ToolDef]:
    if session is None:
        session = ContractSession(contract)
```

to:

```python
def create_tools(
    contract: DataContract,
    *,
    adapter: DatabaseAdapter | None = None,
    semantic_source: SemanticSource | None = None,
    session: ContractSession | None = None,
    caller_principal: Principal = None,
    staleness_threshold_days: int = 90,
    row_format: RowFormat = "compact",
) -> list[ToolDef]:
    # Validated here rather than at render time: tool callables are async and
    # their failures surface as agent-visible text, so a deferred check would
    # turn a wiring typo into a confusing mid-session tool error.
    if row_format not in _ROW_FORMATS:
        raise ValueError(
            f"row_format must be one of {list(_ROW_FORMATS)}; got {row_format!r}"
        )

    rows_note = _COMPACT_ROWS_NOTE if row_format == "compact" else ""

    if session is None:
        session = ContractSession(contract)
```

- [ ] **Step 4: Switch `run_query` to the helper**

At `factory.py:788`, replace:

```python
        rows = [dict(zip(qresult.columns, row)) for row in qresult.rows]
        data = {
            "columns": qresult.columns,
            "rows": rows,
            "row_count": qresult.row_count,
            "session": {"remaining": session.remaining()},
        }
```

with:

```python
        data = {
            "columns": qresult.columns,
            "rows": _render_rows(qresult.columns, qresult.rows, row_format),
            "row_count": qresult.row_count,
            "session": {"remaining": session.remaining()},
        }
```

- [ ] **Step 5: Append the clause to the `run_query` description**

At `factory.py:947`, replace:

```python
            name="run_query",
            description="Validate and execute a SQL query, returning the results.",
```

with:

```python
            name="run_query",
            description=(
                "Validate and execute a SQL query, returning the results."
                + rows_note
            ),
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `uv run pytest tests/test_tools/test_row_format.py -v`
Expected: 14 passed

- [ ] **Step 7: Run the full suite**

Run: `uv run pytest -q`
Expected: green. `tests/test_tools/test_factory.py::test_run_query_valid` asserts `"100" in text`, which holds in both renderings.

- [ ] **Step 8: Commit**

```bash
git add src/agentic_data_contracts/tools/factory.py tests/test_tools/test_row_format.py
git commit -m "feat: run_query renders rows as positional arrays by default

Adds create_tools(row_format=...) with eager validation. Compact is the
default; row_format='records' restores the dict-per-row shape. Refs #44"
```

---

### Task 3: `preview_table` — helper plus a `columns` key in both modes

**Files:**
- Modify: `src/agentic_data_contracts/tools/factory.py:400-401` (rows + envelope), `:832-833` (description)
- Modify: `tests/test_tools/test_factory_principals.py:126-128`
- Test: `tests/test_tools/test_row_format.py` (append)

**Interfaces:**
- Consumes: `_render_rows`, `rows_note` (Task 2).
- Produces: `preview_table` envelope `{"schema", "table", "columns", "rows"}` in both modes.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_tools/test_row_format.py`:

```python
@pytest.mark.asyncio
async def test_preview_table_compact_carries_columns(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    result = await _tool(tools, "preview_table").callable(
        {"schema": "analytics", "table": "orders"}
    )
    body = json.loads(result["content"][0]["text"])
    assert body["columns"] == ["id", "amount", "tenant_id"]
    assert body["rows"][0] == [1, "100.00", "acme"]


@pytest.mark.asyncio
async def test_preview_table_records_also_carries_columns(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(
        contract, adapter=adapter, semantic_source=semantic, row_format="records"
    )
    result = await _tool(tools, "preview_table").callable(
        {"schema": "analytics", "table": "orders"}
    )
    body = json.loads(result["content"][0]["text"])
    assert body["columns"] == ["id", "amount", "tenant_id"]
    assert body["rows"][0] == {"id": 1, "amount": "100.00", "tenant_id": "acme"}


@pytest.mark.asyncio
async def test_zero_row_preview_still_reports_columns(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    # The gap this closes: an empty preview used to return {"rows": []} and
    # tell the agent nothing about the table's shape.
    adapter.connection.execute("DELETE FROM analytics.orders")
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    result = await _tool(tools, "preview_table").callable(
        {"schema": "analytics", "table": "orders"}
    )
    body = json.loads(result["content"][0]["text"])
    assert body["rows"] == []
    assert body["columns"] == ["id", "amount", "tenant_id"]


def test_preview_table_description_documents_compact_shape(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    compact = _tool(create_tools(contract, adapter=adapter), "preview_table")
    records = _tool(
        create_tools(contract, adapter=adapter, row_format="records"), "preview_table"
    )
    assert "positionally aligned" in compact.description
    assert records.description == "Preview sample rows from an allowed table."
```

- [ ] **Step 2: Run the new tests to verify they fail**

Run: `uv run pytest tests/test_tools/test_row_format.py -v -k preview`
Expected: FAIL — `KeyError: 'columns'` on the first three, and an assertion failure on the description test

- [ ] **Step 3: Switch `preview_table` to the helper and add `columns`**

At `factory.py:400`, replace:

```python
        rows = [dict(zip(result.columns, row)) for row in result.rows]
        body = json.dumps({"schema": schema, "table": table, "rows": rows}, default=str)
```

with:

```python
        # `columns` precedes `rows`: json.dumps preserves insertion order, so
        # the model reads the header before the values it must align to.
        body = json.dumps(
            {
                "schema": schema,
                "table": table,
                "columns": result.columns,
                "rows": _render_rows(result.columns, result.rows, row_format),
            },
            default=str,
        )
```

- [ ] **Step 4: Append the clause to the `preview_table` description**

At `factory.py:832`, replace:

```python
            name="preview_table",
            description="Preview sample rows from an allowed table.",
```

with:

```python
            name="preview_table",
            description="Preview sample rows from an allowed table." + rows_note,
```

- [ ] **Step 5: Update the one existing test that reads a row as a dict**

In `tests/test_tools/test_factory_principals.py`, replace lines 126-128:

```python
        body = json.loads(text)
        # DuckDB returns Decimal; json.dumps(..., default=str) renders it as a string.
        assert body["rows"][0]["salary"] == "100000.00"
```

with:

```python
        body = json.loads(text)
        # DuckDB returns Decimal; json.dumps(..., default=str) renders it as a string.
        salary = body["rows"][0][body["columns"].index("salary")]
        assert salary == "100000.00"
```

Line 176's `assert isinstance(body["rows"], list)` needs no change.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `uv run pytest tests/test_tools/test_row_format.py tests/test_tools/test_factory_principals.py -v`
Expected: all pass (18 in `test_row_format.py`)

- [ ] **Step 7: Run the full suite and linters**

Run: `uv run pytest -q && prek run --all-files`
Expected: full suite green, all hooks pass

- [ ] **Step 8: Commit**

```bash
git add src/agentic_data_contracts/tools/factory.py \
        tests/test_tools/test_row_format.py \
        tests/test_tools/test_factory_principals.py
git commit -m "feat: preview_table honours row_format and reports columns

Both modes now emit {schema, table, columns, rows}, so the two result
tools share one envelope and a zero-row preview still describes the
table's shape. Refs #44"
```

---

### Task 4: Forward `row_format` through the four ecosystem wrappers

**Files:**
- Modify: `src/agentic_data_contracts/tools/langchain.py:79-119`
- Modify: `src/agentic_data_contracts/tools/sdk.py:76-143`
- Modify: `src/agentic_data_contracts/tools/pydantic_ai.py:81-121` and `:194-254`
- Test: `tests/test_tools/test_row_format.py` (append)

**Interfaces:**
- Consumes: `create_tools(..., row_format=...)`, `RowFormat`, `_ROW_FORMATS` from Tasks 1-2.
- Produces: `row_format: RowFormat = "compact"` on `create_langchain_tools`, `create_sdk_mcp_server`, `create_pydantic_ai_tools`, `create_pydantic_ai_toolset`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_tools/test_row_format.py`:

```python
def test_pydantic_ai_toolset_validates_row_format_eagerly(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    # The toolset builds its tools inside a per-run factory, so without its own
    # check a typo would surface on the first agent run instead of at wiring.
    pytest.importorskip("pydantic_ai")
    from agentic_data_contracts.tools.pydantic_ai import create_pydantic_ai_toolset

    with pytest.raises(ValueError, match="row_format must be one of"):
        create_pydantic_ai_toolset(
            contract,
            adapter=adapter,
            row_format="bogus",  # type: ignore[arg-type]
        )


def test_langchain_wrapper_accepts_row_format(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    pytest.importorskip("langchain_core")
    from agentic_data_contracts.tools.langchain import create_langchain_tools

    tools = create_langchain_tools(contract, adapter=adapter, row_format="records")
    preview = next(t for t in tools if t.name == "preview_table")
    assert "positionally aligned" not in preview.description


def test_sdk_wrapper_accepts_row_format(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    pytest.importorskip("claude_agent_sdk")
    import inspect as _inspect

    from agentic_data_contracts.tools.sdk import create_sdk_mcp_server

    assert "row_format" in _inspect.signature(create_sdk_mcp_server).parameters


def test_prebuilt_tools_list_takes_precedence_over_row_format(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    pytest.importorskip("langchain_core")
    from agentic_data_contracts.tools.langchain import create_langchain_tools

    prebuilt = create_tools(contract, adapter=adapter, row_format="compact")
    tools = create_langchain_tools(
        contract, adapter=adapter, tools=prebuilt, row_format="records"
    )
    preview = next(t for t in tools if t.name == "preview_table")
    assert "positionally aligned" in preview.description
```

- [ ] **Step 2: Run the new tests to verify they fail**

Run: `uv run pytest tests/test_tools/test_row_format.py -v -k "wrapper or toolset or prebuilt"`
Expected: FAIL — `TypeError: ... got an unexpected keyword argument 'row_format'`

- [ ] **Step 3: Update `create_langchain_tools`**

In `src/agentic_data_contracts/tools/langchain.py`, add `row_format` to the signature after `tools` (line 85) and forward it. The signature becomes:

```python
def create_langchain_tools(
    contract: DataContract,
    *,
    adapter: DatabaseAdapter | None = None,
    semantic_source: SemanticSource | None = None,
    session: ContractSession | None = None,
    tools: list[ToolDef] | None = None,
    apply_middleware: bool = True,
    row_format: RowFormat = "compact",
) -> list[BaseTool]:
```

Import `RowFormat` alongside the existing `create_tools` / `ToolDef` import from `agentic_data_contracts.tools.factory`.

In the docstring `Args:` block, after the `tools:` entry, add:

```
        row_format: How ``run_query`` / ``preview_table`` render result
            rows — ``"compact"`` (default) for positional arrays aligned
            to ``columns``, ``"records"`` for one dict per row. Ignored
            when ``tools`` is supplied.
```

Then change the `create_tools` call (line 114) to:

```python
        tools = create_tools(
            contract,
            adapter=adapter,
            semantic_source=semantic_source,
            session=session,
            row_format=row_format,
        )
```

- [ ] **Step 4: Update `create_sdk_mcp_server`**

In `src/agentic_data_contracts/tools/sdk.py`, add `row_format: RowFormat = "compact"` to the signature after `apply_middleware` (line 83), import `RowFormat` from `agentic_data_contracts.tools.factory`, add the same docstring `Args:` entry as Step 3, and change the `create_tools` call (line 138) to:

```python
        tools = create_tools(
            contract,
            adapter=adapter,
            semantic_source=semantic_source,
            session=session,
            row_format=row_format,
        )
```

- [ ] **Step 5: Update `create_pydantic_ai_tools`**

In `src/agentic_data_contracts/tools/pydantic_ai.py`, add `row_format: RowFormat = "compact"` to the signature after `apply_middleware` (line 89), import `RowFormat` from `agentic_data_contracts.tools.factory`, add the same docstring `Args:` entry, and change the `create_tools` call (line 115) to:

```python
        tools = create_tools(
            contract,
            adapter=adapter,
            semantic_source=semantic_source,
            session=session,
            caller_principal=caller_principal,
            row_format=row_format,
        )
```

- [ ] **Step 6: Update `create_pydantic_ai_toolset` with its own eager check**

In the same file, add `row_format: RowFormat = "compact"` to the signature after `apply_middleware` (line 199). Immediately after the docstring's closing `"""` (line 230) and before `def _factory`, insert:

```python
    # This function returns a factory that builds tools per run, so deferring
    # to create_tools would push a typo to the first agent run. Check now.
    if row_format not in _ROW_FORMATS:
        raise ValueError(
            f"row_format must be one of {list(_ROW_FORMATS)}; got {row_format!r}"
        )

```

Import `_ROW_FORMATS` alongside `RowFormat` from `agentic_data_contracts.tools.factory`. Then change the `create_pydantic_ai_tools` call inside `_factory` (line 244) to:

```python
        tools = create_pydantic_ai_tools(
            contract,
            adapter=adapter,
            semantic_source=semantic_source,
            session=deps.session,
            caller_principal=deps.caller_principal,
            apply_middleware=apply_middleware,
            row_format=row_format,
        )
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `uv run pytest tests/test_tools/test_row_format.py tests/test_tools/test_langchain.py tests/test_tools/test_sdk.py tests/test_tools/test_pydantic_ai.py -v`
Expected: all pass

- [ ] **Step 8: Run the full suite and linters**

Run: `uv run pytest -q && prek run --all-files`
Expected: full suite green, all hooks pass

- [ ] **Step 9: Commit**

```bash
git add src/agentic_data_contracts/tools/langchain.py \
        src/agentic_data_contracts/tools/sdk.py \
        src/agentic_data_contracts/tools/pydantic_ai.py \
        tests/test_tools/test_row_format.py
git commit -m "feat: forward row_format through the ecosystem wrappers

All four wrappers accept and forward row_format; the pydantic-ai toolset
validates it before returning its per-run factory so the fail-at-wiring
guarantee holds on every path. Refs #44"
```

---

### Task 5: Documentation, changelog, version bump

**Files:**
- Modify: `README.md:415`, `:422`, and a new subsection after the tools table
- Modify: `docs/architecture.md` (the `preview_table` and `run_query` entries, ~L357 and ~L364)
- Modify: `CHANGELOG.md` (new entry at the top, after line 4)
- Modify: `pyproject.toml:3`

**Interfaces:**
- Consumes: the finished behaviour from Tasks 1-4.
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Update the README tools table**

In `README.md`, replace line 415:

```
| `preview_table` | Preview sample rows from an allowed table |
```

with:

```
| `preview_table` | Preview sample rows from an allowed table; returns `{schema, table, columns, rows}` |
```

and line 422:

```
| `run_query` | Validate and execute a SQL query, returning results |
```

with:

```
| `run_query` | Validate and execute a SQL query, returning results as `{columns, rows, row_count, session}` |
```

- [ ] **Step 2: Add the README "Result encoding" subsection**

In `README.md`, immediately after the tools table (after line 422, before `## Domain-Driven Agent Workflow` on line 424), insert:

```markdown
### Result encoding

`run_query` and `preview_table` both return a `columns` list alongside their
`rows`. By default `rows` is a list of **positional arrays** aligned to
`columns`:

```json
{"columns": ["region", "units"], "rows": [["EMEA", 412], ["APAC", 87]]}
```

This costs less than half the tokens of repeating every column name on every
row, and matters more than it looks: tool results stay in the message history
and are re-sent on every subsequent model request, so an oversized result is
paid for repeatedly rather than once.

Pass `row_format="records"` to `create_tools()` (or to any of the ecosystem
wrappers) to get one dict per row instead — the pre-0.31 shape:

```python
tools = create_tools(contract, adapter=adapter, row_format="records")
# {"columns": ["region", "units"],
#  "rows": [{"region": "EMEA", "units": 412}, {"region": "APAC", "units": 87}]}
```

Both renderings carry identical information and coerce values identically; only
the container differs. An unrecognised value raises `ValueError` at
`create_tools()` time, not on the first query.
```

- [ ] **Step 3: Update `docs/architecture.md`**

Find the tool inventory entry (~L357):

```
2. **`preview_table(schema, table, limit?)`** — Sample rows
```

replace with:

```
2. **`preview_table(schema, table, limit?)`** — Sample rows, as `{schema, table, columns, rows}`
```

Leave the `run_query` entry (~L364) as it is, and insert a new paragraph
immediately after the numbered list ends:

```markdown
Both result-returning tools render `rows` according to `create_tools(row_format=...)`:
`"compact"` (the default) emits positional arrays aligned to the `columns` key,
`"records"` emits one dict per row. The rendering is an operator decision, not an
agent-facing tool argument — the two carry identical information, so the model has
no basis on which to choose, and a schema field would cost input tokens on every
request. The value is validated eagerly at `create_tools()` time.
```

- [ ] **Step 4: Add the changelog entry**

In `CHANGELOG.md`, insert immediately after line 4 (the blank line following the "All notable changes…" preamble):

```markdown
## [0.31.0] - 2026-07-24

### Changed

- **`run_query` and `preview_table` return compact rows by default.** Both tools previously rendered each result row as a JSON object repeating every column name, which roughly **doubles to triples** the token cost of a result for identical information — measured at 198,003 characters versus 84,503 for a 20-column x 500-row result. Because tool results persist in the message history and are re-sent on every subsequent model request, that overhead is paid repeatedly rather than once. `rows` is now a list of **positional arrays** aligned to the `columns` key that `run_query` already returned. New `create_tools(..., row_format=...)` selects the rendering: `"compact"` (default) or `"records"` (the previous dict-per-row shape). The knob is deliberately **operator-facing rather than a model-facing tool argument** — unlike Anthropic's `concise`/`detailed` pattern this drops no information, so the agent has no basis on which to choose, and an `input_schema` field would cost tokens on every request. An unrecognised value raises `ValueError` at `create_tools()` time. In `compact` mode both tool descriptions gain one clause stating that rows are positionally aligned to `columns`.

### Added

- **`preview_table` now returns a `columns` key** in both renderings, so the two result tools share one `{columns, rows}` envelope and a consumer writes a single parser. This also closes a real gap: a zero-row preview previously returned `{"rows": []}` and told the agent nothing about the table's shape.
- **`RowFormat`** (`Literal["compact", "records"]`) is exported from the package root, alongside `Principal`, for callers typing their own wiring.
- All four ecosystem wrappers — `create_langchain_tools`, `create_sdk_mcp_server`, `create_pydantic_ai_tools`, `create_pydantic_ai_toolset` — accept and forward `row_format`. `create_pydantic_ai_toolset` validates it in its own body rather than deferring to the per-run factory, so the fail-at-wiring-time guarantee holds on every path. A pre-built `tools=` list continues to take precedence, as it already does over `adapter` and `semantic_source`.

### Compatibility

- **The default output shape of `run_query` and `preview_table` changed.** Anything parsing `rows` as a list of dicts must either read positionally (`row[columns.index("col")]`) or pass `row_format="records"` to restore the previous rendering exactly. Values are unaffected — `json.dumps(..., default=str)` is unchanged, so `Decimal` and `date` still serialize identically. `preview_table`'s new `columns` key is additive and present in both modes. Every other tool is untouched, and no new dependencies are added.

### Internal

- New `_render_rows` helper in `tools/factory.py` owns the branch for both tools. Its `list(row)` coercion is load-bearing: `DatabaseAdapter` is a `@runtime_checkable` Protocol, so an adapter may return its driver's row type, which `dict(zip(...))` tolerated (it needs only iteration) but `json.dumps` would have routed through `default=str` and serialized as a string. Built across 5 TDD tasks, red-first, from a reviewed spec. Full suite green; `ruff` / `ruff format` / `ty` clean.

```

- [ ] **Step 5: Bump the version**

In `pyproject.toml`, change line 3 from `version = "0.30.0"` to `version = "0.31.0"`.

- [ ] **Step 6: Verify the docs match reality**

Run: `uv run pytest -q && prek run --all-files`
Expected: full suite green, all hooks pass

Then re-read the README "Result encoding" block and confirm the JSON examples match what Task 2's and Task 3's tests actually assert.

- [ ] **Step 7: Commit**

```bash
git add README.md docs/architecture.md CHANGELOG.md pyproject.toml
git commit -m "docs: document row_format and release 0.31.0

Refs #44"
```

- [ ] **Step 8: Remove the process artifacts**

Per the repo's convention, per-feature specs and plans under `docs/superpowers/` are process artifacts removed once the work ships:

```bash
git rm -r docs/superpowers
git commit -m "chore: remove shipped spec and plan for compact row encoding"
```

- [ ] **Step 9: Final verification**

Run: `uv run pytest -q && prek run --all-files && git log --oneline main..HEAD`
Expected: suite green, hooks pass, 7 commits on the branch. Stop here — do not push or open a PR without asking.

---

## Self-Review

**Spec coverage.** D1 (operator knob, compact default) → Task 2 Step 3. D2 (arrays-of-arrays) → Task 1 Step 4. D3 (`columns` in both modes) → Task 3 Step 3. D4 (description clause) → Tasks 2 Step 5 and 3 Step 4. D5 (wrapper forwarding) → Task 4. `RowFormat` home and export → Task 1 Steps 4-5. Eager validation including the toolset path → Task 2 Step 3 and Task 4 Step 6. `list(row)` coercion → Task 1 Step 4, tested in Task 1 Step 1. Zero-row `columns` caveat → tested in Task 3 Step 1 against DuckDB, which populates `cursor.description`. The existing-test update → Task 3 Step 5. Docs, changelog, version → Task 5. Non-goals are respected: no truncation, no third format, no model-facing argument, no wrapper refactor beyond adding the one parameter.

**Naming consistency.** `RowFormat`, `_ROW_FORMATS`, `_COMPACT_ROWS_NOTE`, `_render_rows`, `rows_note`, and `row_format` are spelled identically in every task. `rows_note` is created in Task 2 Step 3 and consumed in Task 3 Step 4 — Task 3 must not be executed before Task 2.

**Ordering constraint.** Tasks are strictly sequential: 2 depends on 1, 3 on 2, 4 on 2, 5 on all. Only Tasks 3 and 4 are mutually independent.
