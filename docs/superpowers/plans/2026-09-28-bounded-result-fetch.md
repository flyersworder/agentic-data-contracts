# Bounded Result Fetch Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Query tools never materialise more rows than they return, embedded DuckDB can be given a memory ceiling, and the DABStep harness bounds rows, time and memory identically for all four arms (#116).

**Architecture:** A new optional adapter capability, `RowLimitAdapter.execute_limited`, fetches at most N+1 rows in the database. `create_tools(max_result_rows=1000)` sets the cap, and `_execute_bounded` dispatches to the capability or falls back to slicing. The fetch is widened to cover result-check row thresholds, so those checks stay exact. `DuckDBAdapter` implements the capability with `fetchmany`, and gains `memory_limit`.

**Tech Stack:** Python 3.12+, DuckDB (floor 1.1.1), pytest + pytest-asyncio, uv, prek (ruff + ty).

**Spec:** `docs/superpowers/specs/2026-09-28-bounded-result-fetch-design.md`

## Global Constraints

- Branch `fix/bounded-result-fetch`; release version `0.55.0`.
- `max_result_rows` default `1000`; `None` disables; values < 1 raise `ValueError` at wiring.
- `DuckDBAdapter(memory_limit=...)` defaults to `None`, is applied with a parameterised `SET memory_limit = ?` after `connect`, never through `duckdb.connect(config=...)`.
- An untruncated `run_query` payload is byte-identical to 0.54.0's. A truncated one adds `"truncated": true` after `row_count`.
- `DatabaseAdapter` is unchanged. `QueryResult.truncated` is appended as the last field.
- Harness constants: `MAX_ROWS = 50` (unchanged), `HARNESS_QUERY_SECONDS = 120`, `HARNESS_MEMORY_LIMIT = "512MB"`. Marker text: `-- truncated at {MAX_ROWS} rows (more rows not shown)`.
- Never edit `experiments/dabstep-contract-eval/contract/` or `contract_hollow/` (their digests are published), FINDINGS.md, or `docs/paper/`.
- Run tools with `uv run`; lint through `prek run --all-files`, and `git add` new files first (prek skips untracked files).
- Do not write `file.py:NNN` references in code or docs; a pygrep hook rejects them. Name the symbol.
- Verified on DuckDB 1.5.5 and 1.1.1: `execute(...).fetchmany(51)` on a 10^10-row join returns in 0.01 s at about 50 MB RSS; `SET memory_limit = ?` accepts a parameter; exceeding it raises `duckdb.OutOfMemoryException` and leaves the connection usable; an invalid value raises `ParserException`.

## Review Focus

1. **Result checks on a truncated result.** A `min_rows`/`max_rows` threshold above the cap must give the same verdict as without the cap. Pinned in Task 3 (`test_row_thresholds_above_the_cap_stay_exact`).
2. **Small results must not change.** Byte-identical payload, including `row_count` an adapter set explicitly. Pinned in Task 3 (`test_untruncated_payload_is_unchanged`).
3. **A timeout during a limited fetch.** It must still be a cancelled `QueryTimeoutError`, and the adapter must serve the next query. Pinned in Task 1 (`test_execute_limited_honours_timeout`).
4. **A bad `memory_limit` string** must fail at construction, not on the first query. Pinned in Task 2 (`test_invalid_memory_limit_fails_at_construction`).
5. **Third-party adapters without the capability** still get a capped result, plus one warning per class. Pinned in Task 3 (`test_fallback_slices_and_warns_once`).

---

### Task 1: `RowLimitAdapter`, `QueryResult.truncated`, DuckDB `execute_limited`

**Files:**
- Modify: `src/agentic_data_contracts/adapters/base.py` (`QueryResult`, new `RowLimitAdapter`, `__all__`)
- Modify: `src/agentic_data_contracts/adapters/__init__.py` (export)
- Modify: `src/agentic_data_contracts/adapters/duckdb.py` (`_interrupt_after` helper, `execute_with_timeout`, new `execute_limited`)
- Test: `tests/test_adapters/test_duckdb.py`, `tests/test_public_api.py`

**Interfaces:**
- Produces: `QueryResult(columns, rows, row_count=0, truncated=False)`; `RowLimitAdapter` protocol with `execute_limited(self, sql: str, max_rows: int, timeout_seconds: float | None = None) -> QueryResult`; `DuckDBAdapter.execute_limited` with that signature.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_adapters/test_duckdb.py` (reuses the file's `adapter` fixture and `SLOW_SQL`; add `RowLimitAdapter` to the `adapters.base` import):

```python
# 10^10 rows: returning quickly proves nothing past the cap was materialised.
HUGE_SQL = "SELECT a.range AS x, b.range AS y FROM range(100000) a, range(100000) b"


def test_adapter_supports_row_limit(adapter: DuckDBAdapter) -> None:
    assert isinstance(adapter, RowLimitAdapter)


def test_query_result_truncated_defaults_false() -> None:
    assert QueryResult(columns=["a"], rows=[(1,)]).truncated is False


def test_execute_limited_stops_at_the_cap(adapter: DuckDBAdapter) -> None:
    start = time.monotonic()
    result = adapter.execute_limited(HUGE_SQL, 50)
    assert time.monotonic() - start < 2
    assert result.columns == ["x", "y"]
    assert len(result.rows) == 50
    assert result.row_count == 50
    assert result.truncated is True


def test_execute_limited_exact_fit_is_not_truncated(adapter: DuckDBAdapter) -> None:
    result = adapter.execute_limited("SELECT range FROM range(5)", 5)
    assert len(result.rows) == 5
    assert result.truncated is False


def test_execute_limited_small_result(adapter: DuckDBAdapter) -> None:
    result = adapter.execute_limited("SELECT id FROM analytics.orders ORDER BY id", 50)
    assert result.rows == [(1,), (2,)]
    assert result.truncated is False


def test_execute_limited_rejects_a_non_positive_cap(adapter: DuckDBAdapter) -> None:
    with pytest.raises(ValueError, match="max_rows"):
        adapter.execute_limited("SELECT 1", 0)


def test_execute_limited_honours_timeout(adapter: DuckDBAdapter) -> None:
    start = time.monotonic()
    with pytest.raises(QueryTimeoutError) as exc_info:
        adapter.execute_limited(SLOW_SQL, 10, timeout_seconds=0.3)
    assert time.monotonic() - start < 5
    assert exc_info.value.cancelled is True
    assert adapter.execute_limited("SELECT 42", 10).rows == [(42,)]


def test_connection_usable_after_a_truncated_fetch(adapter: DuckDBAdapter) -> None:
    adapter.execute_limited(HUGE_SQL, 3)
    assert adapter.execute("SELECT 42").rows == [(42,)]
```

In `tests/test_public_api.py::test_adapter_imports`, add `RowLimitAdapter,  # noqa: F401` to the `adapters.base` import list.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_adapters/test_duckdb.py tests/test_public_api.py -q`
Expected: FAIL with `ImportError: cannot import name 'RowLimitAdapter'`.

- [ ] **Step 3: Add the types to `base.py`**

Replace `QueryResult` with:

```python
@dataclass
class QueryResult:
    columns: list[str]
    rows: list[tuple[Any, ...]]
    row_count: int = 0
    # Appended so positional construction keeps binding as before. True when
    # the query produced more rows than were fetched: `rows` is a prefix and
    # `row_count` counts only it.
    truncated: bool = False

    def __post_init__(self) -> None:
        if self.row_count == 0:
            self.row_count = len(self.rows)
```

After `TimeoutAdapter`, add:

```python
@runtime_checkable
class RowLimitAdapter(Protocol):
    """Optional adapter capability: fetch at most ``max_rows`` rows.

    Detected at runtime, so ``DatabaseAdapter`` stays unchanged. Read at most
    ``max_rows + 1`` rows from the database and return at most ``max_rows``,
    with ``truncated=True`` when the extra row existed; never read the rest.
    DB-API drivers do this with ``cursor.fetchmany(max_rows + 1)``.

    The query tools pass ``timeout_seconds`` only to an adapter that also
    implements ``TimeoutAdapter``, which must then honour it as
    ``execute_with_timeout`` does. Any other adapter receives ``None`` and gets
    the tools' caller-side deadline instead. Without this capability the tools
    still cap what the agent sees, but only after ``execute`` has fetched
    everything.
    """

    def execute_limited(
        self, sql: str, max_rows: int, timeout_seconds: float | None = None
    ) -> QueryResult: ...
```

Add `"RowLimitAdapter"` to `__all__` in `base.py` and export it from `adapters/__init__.py` (both the import list and `__all__`, alphabetical).

- [ ] **Step 4: Refactor the DuckDB watchdog and add `execute_limited`**

In `duckdb.py`, add `from collections.abc import Iterator` and `from contextlib import contextmanager, nullcontext`. Replace `execute_with_timeout` with the helper plus two methods:

```python
@contextmanager
def _interrupt_after(self, timeout_seconds: float) -> Iterator[None]:
    """Interrupt the connection if the body runs past ``timeout_seconds``.

    The caller must hold ``_lock``: ``interrupt()`` cancels whatever the
    shared connection is running, so a clock started while still waiting
    for the lock would cancel another caller's statement.
    """
    done = threading.Event()
    timed_out = threading.Event()
    # Held while interrupting and while marking the call done, so no
    # interrupt can land after the lock is released -- on the next
    # caller's statement.
    guard = threading.Lock()

    def _watchdog() -> None:
        # Clamped: a longer wait raises OverflowError in this thread,
        # silently leaving the limit unarmed.
        if done.wait(min(timeout_seconds, threading.TIMEOUT_MAX)):
            return
        timed_out.set()
        # DuckDB drops an interrupt sent while the connection is idle,
        # so one fired during work `execute` does before its statement
        # (a subclass rewriting SQL) would be lost. Repeat until the
        # call returns.
        while True:
            with guard:
                if done.is_set():
                    return
                self.connection.interrupt()
            if done.wait(_REINTERRUPT_SECONDS):
                return

    threading.Thread(target=_watchdog, daemon=True).start()
    try:
        yield
    except Exception as e:
        # The timeout is an interrupt after the deadline, even one a
        # subclass's `execute` re-raised as another type. Any other
        # error -- memory_limit, say -- reaches the agent as itself.
        if timed_out.is_set() and _caused_by_interrupt(e):
            raise QueryTimeoutError(timeout_seconds) from None
        raise
    finally:
        with guard:
            done.set()


def execute_with_timeout(self, sql: str, timeout_seconds: float) -> QueryResult:
    """Run ``sql``, interrupting it after ``timeout_seconds``.

    Runs through ``self.execute``, so a subclass override (SQL rewriting,
    auditing) stays on the path.
    """
    with self._lock, self._interrupt_after(timeout_seconds):
        return self.execute(sql)


def execute_limited(
    self, sql: str, max_rows: int, timeout_seconds: float | None = None
) -> QueryResult:
    """Run ``sql`` and fetch at most ``max_rows`` rows.

    DuckDB streams the result, so rows past ``max_rows + 1`` are never
    produced. Does not call ``self.execute``: a subclass that rewrites SQL
    there must override this method too.
    """
    if max_rows < 1:
        raise ValueError(f"max_rows must be at least 1, got {max_rows}")
    with self._lock:
        limit = (
            self._interrupt_after(timeout_seconds)
            if timeout_seconds is not None
            else nullcontext()
        )
        with limit:
            result = self.connection.execute(sql)
            columns = [desc[0] for desc in result.description]
            rows = result.fetchmany(max_rows + 1)
    return QueryResult(
        columns=columns,
        rows=rows[:max_rows],
        truncated=len(rows) > max_rows,
    )
```

Update the `_lock` comment in `__init__` to say `execute_with_timeout` holds the lock while calling `self.execute`.

- [ ] **Step 5: Run the adapter suite (new tests and the existing timeout tests)**

Run: `uv run pytest tests/test_adapters tests/test_public_api.py tests/test_tools/test_query_timeout.py -q`
Expected: all PASS. The existing `execute_with_timeout` tests are the refactor's regression net.

- [ ] **Step 6: Commit**

```bash
git add src/agentic_data_contracts/adapters tests/test_adapters/test_duckdb.py tests/test_public_api.py
git commit -m "feat(adapters): RowLimitAdapter capability; DuckDB fetches at most N rows (#116)"
```

---

### Task 2: `DuckDBAdapter(memory_limit=...)`

**Files:**
- Modify: `src/agentic_data_contracts/adapters/duckdb.py` (`__init__`, class docstring)
- Test: `tests/test_adapters/test_duckdb.py`

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces: `DuckDBAdapter(database: str = ":memory:", *, memory_limit: str | None = None)`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_adapters/test_duckdb.py` (add `import duckdb` at the top):

```python
def _setting(db: DuckDBAdapter, name: str) -> str:
    return db.connection.execute("SELECT current_setting(?)", [name]).fetchone()[0]


def test_memory_limit_is_applied() -> None:
    db = DuckDBAdapter(":memory:", memory_limit="64MB")
    assert _setting(db, "memory_limit") == "61.0 MiB"


def test_memory_limit_default_leaves_duckdb_default() -> None:
    assert _setting(DuckDBAdapter(":memory:"), "memory_limit") == _setting(
        DuckDBAdapter(":memory:", memory_limit=None), "memory_limit"
    )


def test_runaway_query_is_an_engine_error_and_the_adapter_recovers() -> None:
    db = DuckDBAdapter(":memory:", memory_limit="50MB")
    # list() cannot spill to disk, so it hits the limit instead of paging.
    with pytest.raises(duckdb.OutOfMemoryException):
        db.execute_limited("SELECT list(range) FROM range(100000000)", 10)
    assert db.execute("SELECT 42").rows == [(42,)]


def test_invalid_memory_limit_fails_at_construction() -> None:
    with pytest.raises(duckdb.Error):
        DuckDBAdapter(":memory:", memory_limit="bogus")
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_adapters/test_duckdb.py -q -k memory`
Expected: FAIL with `TypeError: DuckDBAdapter.__init__() got an unexpected keyword argument 'memory_limit'`.

- [ ] **Step 3: Implement**

```python
    def __init__(
        self, database: str = ":memory:", *, memory_limit: str | None = None
    ) -> None:
        self.connection = duckdb.connect(database)
        # SET after connect, not connect(config=...): a second in-process
        # connection to the same file with a different config raises.
        if memory_limit is not None:
            self.connection.execute("SET memory_limit = ?", [memory_limit])
        # (existing _lock comment and assignment unchanged)
```

Add this paragraph to the class docstring:

```
    ``memory_limit`` (a DuckDB size string such as ``"512MB"``) caps the
    engine's memory, so a runaway query fails with an out-of-memory error the
    agent sees instead of exhausting the process. It does not bound the Python
    objects built from a result; ``execute_limited`` does that. Unset, DuckDB
    uses its own default of 80% of RAM.
```

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/test_adapters -q`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/adapters/duckdb.py tests/test_adapters/test_duckdb.py
git commit -m "feat(duckdb): memory_limit constructor argument (#116)"
```

---

### Task 3: `create_tools(max_result_rows=...)` in `run_query` and `preview_table`

**Files:**
- Modify: `src/agentic_data_contracts/tools/factory.py` (`_execute_bounded`, new `_with_deadline`, `_result_check_row_threshold`, `validate_max_result_rows`, `_warn_unbounded_fetch`, `create_tools`, `run_query`, `preview_table`)
- Create: `tests/test_tools/test_result_rows.py`

**Interfaces:**
- Consumes: `RowLimitAdapter`, `QueryResult.truncated` (Task 1).
- Produces: `create_tools(..., max_result_rows: int | None = 1000)`; `validate_max_result_rows(max_result_rows: int | None) -> None` in `tools/factory.py` (Task 4 imports it).

- [ ] **Step 1: Write the failing tests**

Create `tests/test_tools/test_result_rows.py`:

```python
"""run_query / preview_table cap the rows they fetch and return (#116)."""

import json
from typing import Any

import pytest

from agentic_data_contracts.adapters.base import QueryResult, TableSchema
from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.schema import (
    AllowedTable,
    DataContractSchema,
    ResultCheck,
    SemanticConfig,
    SemanticRule,
)
from agentic_data_contracts.tools import factory
from agentic_data_contracts.tools.factory import create_tools
from agentic_data_contracts.validation.explain import ExplainResult


def _contract(*rules: SemanticRule) -> DataContract:
    return DataContract(
        DataContractSchema(
            name="test",
            semantic=SemanticConfig(
                allowed_tables=[
                    AllowedTable.model_validate(
                        {"schema": "analytics", "tables": ["orders"]}
                    )
                ],
                rules=list(rules),
            ),
        )
    )


def _row_rule(**check: int) -> SemanticRule:
    return SemanticRule(
        name="rows_" + "_".join(check),  # distinct per rule
        description="row bound",
        enforcement="block",
        result_check=ResultCheck(**check),
    )


@pytest.fixture
def adapter() -> DuckDBAdapter:
    db = DuckDBAdapter(":memory:")
    db.connection.execute(
        "CREATE SCHEMA analytics;"
        " CREATE TABLE analytics.orders AS SELECT range AS id FROM range(100);"
    )
    return db


def _tool(tools: list[Any], name: str) -> Any:
    return next(t for t in tools if t.name == name).callable


def _payload(result: dict[str, Any]) -> dict[str, Any]:
    return json.loads(result["content"][0]["text"])


SQL = "SELECT id FROM analytics.orders ORDER BY id"


async def test_default_cap_is_1000() -> None:
    db = DuckDBAdapter(":memory:")
    db.connection.execute(
        "CREATE SCHEMA analytics;"
        " CREATE TABLE analytics.orders AS SELECT range AS id FROM range(5000);"
    )
    run_query = _tool(create_tools(_contract(), adapter=db), "run_query")
    data = _payload(await run_query({"sql": SQL}))
    assert len(data["rows"]) == 1000
    assert data["row_count"] == 1000
    assert data["truncated"] is True


async def test_truncated_payload_key_order(adapter: DuckDBAdapter) -> None:
    run_query = _tool(
        create_tools(_contract(), adapter=adapter, max_result_rows=10), "run_query"
    )
    data = _payload(await run_query({"sql": SQL}))
    assert list(data) == ["columns", "rows", "row_count", "truncated", "session"]
    assert data["rows"] == [[i] for i in range(10)]


async def test_untruncated_payload_is_unchanged(adapter: DuckDBAdapter) -> None:
    capped = _tool(
        create_tools(_contract(), adapter=adapter, max_result_rows=100), "run_query"
    )
    uncapped = _tool(
        create_tools(_contract(), adapter=adapter, max_result_rows=None), "run_query"
    )
    a = await capped({"sql": SQL})
    b = await uncapped({"sql": SQL})
    assert a["content"][0]["text"] == b["content"][0]["text"]
    assert "truncated" not in a["content"][0]["text"]


async def test_none_returns_every_row(adapter: DuckDBAdapter) -> None:
    run_query = _tool(
        create_tools(_contract(), adapter=adapter, max_result_rows=None), "run_query"
    )
    assert len(_payload(await run_query({"sql": SQL}))["rows"]) == 100


@pytest.mark.parametrize("bad", [0, -1])
def test_non_positive_cap_raises_at_wiring(adapter: DuckDBAdapter, bad: int) -> None:
    with pytest.raises(ValueError, match="max_result_rows"):
        create_tools(_contract(), adapter=adapter, max_result_rows=bad)


async def test_row_thresholds_above_the_cap_stay_exact(adapter: DuckDBAdapter) -> None:
    # 100 rows exist; the cap is 5. Without widening the fetch, min_rows would
    # see 5 rows and block, and max_rows would see 5 and pass.
    passes = _tool(
        create_tools(
            _contract(_row_rule(min_rows=20)), adapter=adapter, max_result_rows=5
        ),
        "run_query",
    )
    result = await passes({"sql": SQL})
    assert "is_error" not in result
    assert len(_payload(result)["rows"]) == 5

    blocks = _tool(
        create_tools(
            _contract(_row_rule(max_rows=20)), adapter=adapter, max_result_rows=5
        ),
        "run_query",
    )
    result = await blocks({"sql": SQL})
    assert result["_kind"] == "blocked"
    assert "maximum is 20" in result["content"][0]["text"]


class _SpyAdapter(DuckDBAdapter):
    def __init__(self) -> None:
        super().__init__(":memory:")
        self.limits: list[int] = []

    def execute_limited(
        self, sql: str, max_rows: int, timeout_seconds: float | None = None
    ) -> QueryResult:
        self.limits.append(max_rows)
        return super().execute_limited(sql, max_rows, timeout_seconds)


async def test_fetch_is_widened_only_to_the_threshold() -> None:
    db = _SpyAdapter()
    db.connection.execute(
        "CREATE SCHEMA analytics;"
        " CREATE TABLE analytics.orders AS SELECT range AS id FROM range(100);"
    )
    rules = (_row_rule(min_rows=20), _row_rule(max_rows=30))
    run_query = _tool(
        create_tools(_contract(*rules), adapter=db, max_result_rows=5), "run_query"
    )
    await run_query({"sql": SQL})
    assert db.limits == [31]


class _PlainAdapter:
    """A DatabaseAdapter with neither execute_limited nor execute_with_timeout."""

    dialect = "duckdb"

    def __init__(self, inner: DuckDBAdapter) -> None:
        self._inner = inner

    def execute(self, sql: str) -> QueryResult:
        return self._inner.execute(sql)

    def explain(self, sql: str) -> ExplainResult:
        return self._inner.explain(sql)

    def describe_table(self, schema: str, table: str) -> TableSchema:
        return self._inner.describe_table(schema, table)

    def list_tables(self, schema: str) -> list[str]:
        return self._inner.list_tables(schema)


async def test_fallback_slices_and_warns_once(
    adapter: DuckDBAdapter,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(factory, "_WARNED_UNBOUNDED_FETCH", set())
    plain = _PlainAdapter(adapter)
    tools = create_tools(_contract(), adapter=plain, max_result_rows=10)
    create_tools(_contract(), adapter=plain, max_result_rows=10)
    assert caplog.text.count("execute_limited") == 1

    data = _payload(await _tool(tools, "run_query")({"sql": SQL}))
    assert len(data["rows"]) == 10
    assert data["truncated"] is True


def test_no_warning_when_uncapped_or_capable(
    adapter: DuckDBAdapter,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(factory, "_WARNED_UNBOUNDED_FETCH", set())
    create_tools(_contract(), adapter=_PlainAdapter(adapter), max_result_rows=None)
    create_tools(_contract(), adapter=adapter)
    assert "execute_limited" not in caplog.text


async def test_preview_table_is_capped(adapter: DuckDBAdapter) -> None:
    preview = _tool(
        create_tools(_contract(), adapter=adapter, max_result_rows=3), "preview_table"
    )
    result = await preview({"schema": "analytics", "table": "orders", "limit": 50})
    assert len(json.loads(result["content"][0]["text"])["rows"]) == 3
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_tools/test_result_rows.py -q`
Expected: FAIL. Most fail with `TypeError: create_tools() got an unexpected keyword argument 'max_result_rows'`. `test_default_cap_is_1000` fails on `len(rows) == 5000`.

- [ ] **Step 3: Add the helpers to `factory.py`**

Import `RowLimitAdapter` alongside `TimeoutAdapter`, and `functools` and `Callable` if not already imported. Replace `_execute_bounded` with:

```python
async def _with_deadline[T](call: Callable[[], T], timeout_seconds: float) -> T:
    """Answer within ``timeout_seconds`` even though ``call`` cannot be
    cancelled: the worker thread runs on until the database finishes it."""
    deadline = asyncio.timeout(timeout_seconds)
    try:
        async with deadline:
            return await asyncio.to_thread(call)
    except TimeoutError:
        # Only our own deadline is the contract's limit. A driver's TimeoutError
        # (socket.timeout is an alias) is an engine failure and must reach the
        # agent as one, not as advice to lighten a query that may be fine.
        if deadline.expired():
            raise QueryTimeoutError(timeout_seconds, cancelled=False) from None
        raise


async def _execute_bounded(
    adapter: DatabaseAdapter,
    sql: str,
    timeout_seconds: float | None,
    *,
    timeout_adapter: TimeoutAdapter | None,
    row_limit_adapter: RowLimitAdapter | None = None,
    max_rows: int | None = None,
) -> QueryResult:
    """Run ``sql`` off the event loop under ``resources.max_query_time_seconds``,
    returning at most ``max_rows`` rows when it is set.

    ``timeout_adapter`` / ``row_limit_adapter`` are ``adapter`` when it
    implements that capability (checked once, at wiring), else ``None``. A
    ``TimeoutAdapter`` enforces the limit itself and cancels the statement in
    the database. Any other adapter gets a caller-side timeout: the agent is
    answered on time, but the statement -- and the worker thread waiting on it
    -- runs on until the database finishes it. Two consequences an adapter
    author should know: the agent's next query can reach the adapter while the
    abandoned one is still running, so an adapter over a single
    non-thread-safe connection must lock it; and abandoned statements hold
    default-executor threads, so enough of them delay every offloaded call.
    Implementing ``execute_with_timeout`` avoids both.

    A ``RowLimitAdapter`` never fetches past ``max_rows``. Any other adapter
    fetches everything and is sliced here: the agent's result is bounded, the
    process's memory is not.
    """
    call: Callable[[], QueryResult]
    if max_rows is not None and row_limit_adapter is not None:
        # Pass the limit only to an adapter that can cancel; any other gets the
        # caller-side deadline below, as `execute` does.
        cancels = timeout_adapter is not None
        call = functools.partial(
            row_limit_adapter.execute_limited,
            sql,
            max_rows,
            timeout_seconds if cancels else None,
        )
        if timeout_seconds is None or cancels:
            return await asyncio.to_thread(call)
        return await _with_deadline(call, timeout_seconds)

    if timeout_seconds is None:
        result = await asyncio.to_thread(adapter.execute, sql)
    elif timeout_adapter is not None:
        result = await asyncio.to_thread(
            timeout_adapter.execute_with_timeout, sql, timeout_seconds
        )
    else:
        result = await _with_deadline(
            functools.partial(adapter.execute, sql), timeout_seconds
        )
    if max_rows is not None and len(result.rows) > max_rows:
        return QueryResult(
            columns=result.columns, rows=list(result.rows[:max_rows]), truncated=True
        )
    return result
```

After `_warn_uncancellable`, add:

```python
# Adapter classes already warned about; see _WARNED_UNCANCELLABLE.
_WARNED_UNBOUNDED_FETCH: set[type] = set()


def _warn_unbounded_fetch(adapter: DatabaseAdapter) -> None:
    key = type(adapter)
    if key in _WARNED_UNBOUNDED_FETCH:
        return
    _WARNED_UNBOUNDED_FETCH.add(key)
    logger.warning(
        "%s does not implement execute_limited: the query tools cap the rows"
        " they return, but every row is fetched into memory first. Implement"
        " execute_limited (e.g. with cursor.fetchmany) to bound memory.",
        key.__name__,
    )


def validate_max_result_rows(max_result_rows: int | None) -> None:
    if max_result_rows is not None and max_result_rows < 1:
        raise ValueError(
            f"max_result_rows must be at least 1, or None for no cap;"
            f" got {max_result_rows}"
        )


def _result_check_row_threshold(contract: DataContract) -> int:
    """The largest ``min_rows``/``max_rows`` any result check declares, or 0.

    A capped fetch reads past this, so a truncated result has provably more
    rows than every threshold and the row-count checks stay exact.
    """
    return max(
        (
            bound
            for rule in contract.schema.semantic.rules
            if rule.result_check is not None
            for bound in (rule.result_check.min_rows, rule.result_check.max_rows)
            if bound is not None
        ),
        default=0,
    )
```

- [ ] **Step 4: Wire it into `create_tools`**

Add the parameter after `row_format`: `max_result_rows: int | None = 1000,`. Directly after `validate_row_format(row_format)`, add `validate_max_result_rows(max_result_rows)`. After the `timeout_adapter` lines, add:

```python
    row_limit_adapter = adapter if isinstance(adapter, RowLimitAdapter) else None
    if max_result_rows is not None and adapter is not None and row_limit_adapter is None:
        _warn_unbounded_fetch(adapter)
    # Rows to fetch: past the cap only as far as the result checks need. See
    # _result_check_row_threshold.
    fetch_rows = (
        max(max_result_rows, _result_check_row_threshold(contract) + 1)
        if max_result_rows is not None
        else None
    )
```

- [ ] **Step 5: Use it in `run_query`**

Change the `_execute_bounded` call to:

```python
                qresult = await _execute_bounded(
                    adapter,
                    sql,
                    max_query_time,
                    timeout_adapter=timeout_adapter,
                    row_limit_adapter=row_limit_adapter,
                    max_rows=fetch_rows,
                )
```

Leave the result-check call unchanged: it must see every fetched row. Replace the block from `data = {` through `response_text = json.dumps(...)` with:

```python
            # Result checks saw every fetched row; the agent sees the cap.
            shown_rows = qresult.rows
            truncated = qresult.truncated
            if max_result_rows is not None and len(shown_rows) > max_result_rows:
                shown_rows = shown_rows[:max_result_rows]
                truncated = True
            data: dict[str, Any] = {
                "columns": qresult.columns,
                "rows": _render_rows(qresult.columns, shown_rows, row_format),
                # Untruncated, the adapter's own count, as before. Truncated,
                # the true total is unknown and counting it would cost what
                # the query did, so report the rows returned.
                "row_count": len(shown_rows) if truncated else qresult.row_count,
            }
            if truncated:
                data["truncated"] = True
            data["session"] = {"remaining": session.remaining()}
            response_text = json.dumps(data, default=str)
```

In the same function, change `_scalar_value(qresult.columns, qresult.rows, "run_query")` to `_scalar_value(qresult.columns, shown_rows, "run_query")`, and the final `_record("ok", ..., row_count=qresult.row_count, ...)` to `row_count=data["row_count"]`.

- [ ] **Step 6: Cap `preview_table`**

After the `limit = ...` try/except in `preview_table`, add:

```python
            if max_result_rows is not None:
                limit = min(limit, max_result_rows)
```

Its `_execute_bounded` call stays as it is: the SQL `LIMIT` already bounds the fetch.

- [ ] **Step 7: Run the new tests and the whole tool suite**

Run: `uv run pytest tests/test_tools -q`
Expected: all PASS. If an existing test asserts more than 1000 rows from `run_query`, pass `max_result_rows=None` in that test rather than weakening the default, and note it in the commit message.

- [ ] **Step 8: Commit**

```bash
git add src/agentic_data_contracts/tools/factory.py tests/test_tools/test_result_rows.py
git commit -m "feat(tools): cap rows fetched and returned by run_query/preview_table (#116)"
```

---

### Task 4: Pass `max_result_rows` through every entry point

**Files:**
- Modify: `src/agentic_data_contracts/tools/sdk.py` (`create_sdk_mcp_server`)
- Modify: `src/agentic_data_contracts/tools/langchain.py` (`create_langchain_tools`)
- Modify: `src/agentic_data_contracts/tools/pydantic_ai.py` (`create_pydantic_ai_tools`, `create_pydantic_ai_toolset`)
- Test: `tests/test_tools/test_result_rows.py` (append)

**Interfaces:**
- Consumes: `create_tools(..., max_result_rows=...)` and `validate_max_result_rows` from Task 3.
- Produces: the same keyword, default `1000`, on all four builders.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_tools/test_result_rows.py`:

```python
@pytest.mark.parametrize(
    ("module", "builder"),
    [
        ("agentic_data_contracts.tools.pydantic_ai", "create_pydantic_ai_tools"),
        ("agentic_data_contracts.tools.langchain", "create_langchain_tools"),
        ("agentic_data_contracts.tools.sdk", "create_sdk_mcp_server"),
    ],
)
def test_entry_points_forward_max_result_rows(
    module: str, builder: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    import importlib

    seen: dict[str, Any] = {}

    def _spy(*args: Any, **kwargs: Any) -> list[Any]:
        seen.update(kwargs)
        return []

    try:
        mod = importlib.import_module(module)
        monkeypatch.setattr(mod, "create_tools", _spy)
        getattr(mod, builder)(_contract(), max_result_rows=7)
    except ImportError:
        pytest.skip(f"{module}'s optional dependency is not installed")
    assert seen["max_result_rows"] == 7


def test_toolset_rejects_a_bad_cap_at_construction() -> None:
    from agentic_data_contracts.tools.pydantic_ai import create_pydantic_ai_toolset

    with pytest.raises(ValueError, match="max_result_rows"):
        create_pydantic_ai_toolset(_contract(), max_result_rows=0)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_tools/test_result_rows.py -q -k "entry_points or toolset"`
Expected: FAIL with `TypeError: ... got an unexpected keyword argument 'max_result_rows'`.

- [ ] **Step 3: Implement**

In each of the four builders, add `max_result_rows: int | None = 1000,` directly after `row_format`, and pass `max_result_rows=max_result_rows` wherever `row_format=row_format` is passed on. In each docstring, directly after the `row_format:` entry, add:

```
        max_result_rows: The most rows ``run_query`` / ``preview_table``
            return (default 1000); ``None`` for no cap. See ``create_tools``.
            Ignored when ``tools`` is supplied.
```

(Omit the last sentence for `create_pydantic_ai_toolset`, which has no `tools` argument.) In `create_pydantic_ai_toolset`, import `validate_max_result_rows` next to `validate_row_format` and call it right after `validate_row_format(row_format)`, for the same reason given in the comment above that line.

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/test_tools -q`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/tools tests/test_tools/test_result_rows.py
git commit -m "feat(tools): forward max_result_rows through SDK, LangChain and Pydantic AI builders (#116)"
```

---

### Task 5: DABStep harness: the same row, time and memory bounds on every arm

**Files:**
- Modify: `experiments/dabstep-contract-eval/dce/arms.py`
- Modify: `experiments/dabstep-contract-eval/tests/test_arms.py`

**Interfaces:**
- Consumes: `DuckDBAdapter(memory_limit=)`, `execute_limited`, `QueryResult.truncated` (Tasks 1–2); `create_tools(max_result_rows=)` (Task 3).
- Produces: `HARNESS_QUERY_SECONDS`, `HARNESS_MEMORY_LIMIT`, `_BoundedDuckDBAdapter`, `_append_truncation_marker` in `dce/arms.py`.

- [ ] **Step 1: Update the tests first**

In `test_truncation_marker_present_when_a_result_is_cut`, replace both `"-- truncated at 3 rows (10 total)"` assertions with `"-- truncated at 3 rows (more rows not shown)"`, and replace the comment above the first with:

```python
    # Every arm reads the same marker, and none of them counts the rest: the
    # count would cost as much as the query it protects against.
```

Append:

```python
def test_every_arm_gets_the_same_marker(db, monkeypatch):
    import dce.arms as arms_mod

    monkeypatch.setattr(arms_mod, "MAX_ROWS", 3)
    con = duckdb.connect(str(db))
    con.execute(
        "CREATE OR REPLACE TABLE payments AS "
        "SELECT range AS psp_reference FROM range(10)"
    )
    con.close()

    markers = []
    for arm in ARMS:
        setup = build_arm(arm, db, DOCS)
        if setup.session is None:
            out = _tool(setup, "execute_sql").function(
                "SELECT psp_reference FROM payments"
            )
        else:
            out = asyncio.run(
                _tool(setup, "run_query").function(
                    _CTX, sql="SELECT psp_reference FROM main.payments"
                )
            )
        markers.append(out.splitlines()[-1])
        setup.close()
    assert set(markers) == {"-- truncated at 3 rows (more rows not shown)"}


def test_execute_sql_is_bounded_in_time(db, monkeypatch):
    import time

    import dce.arms as arms_mod

    monkeypatch.setattr(arms_mod, "HARNESS_QUERY_SECONDS", 0.3)
    setup = build_arm("schema_only", db, DOCS)
    start = time.monotonic()
    out = _tool(setup, "execute_sql").function(
        "SELECT sum(hash(a.range * b.range)) FROM range(100000) a, range(100000) b"
    )
    assert time.monotonic() - start < 5
    assert out.startswith("ERROR:")
    setup.close()


def test_governed_arms_share_the_harness_bounds(db):
    import dce.arms as arms_mod

    setup = build_arm("contract", db, DOCS)
    assert isinstance(setup.adapter, arms_mod._BoundedDuckDBAdapter)
    limit = setup.adapter.connection.execute(
        "SELECT current_setting('memory_limit')"
    ).fetchone()[0]
    assert limit == "488.2 MiB"  # DuckDB's rendering of 512MB
    setup.close()
```

The `contract` arms' `run_query` must pass validation for this SQL. If the frozen contract blocks it, use a query the existing governed test already runs successfully.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd experiments/dabstep-contract-eval && uv run pytest tests/test_arms.py -q`
Expected: FAIL. The marker tests fail on `(10 total)` and the other tests fail with `AttributeError: ... HARNESS_QUERY_SECONDS` / `_BoundedDuckDBAdapter`.

- [ ] **Step 3: Implement the harness bounds**

In `dce/arms.py`:

1. Move `from agentic_data_contracts.adapters.duckdb import DuckDBAdapter` out of the `if TYPE_CHECKING:` block into the module imports. `dce.frozen` already imports the library at module load, so this costs nothing. Drop `TYPE_CHECKING` if nothing else uses it.
2. Rewrite the `MAX_ROWS` comment's parenthetical so it describes the new mechanism: arms A/B cap `execute_sql` through `_BoundedDuckDBAdapter.execute_limited`, and arm C's `run_query` is capped by `create_tools(max_result_rows=MAX_ROWS)`. Delete the final sentence claiming the marker reports the true total, and add: "The marker says only that more rows exist. Counting them meant draining the cursor, which on a runaway join built every row in Python (#116)."
3. After `MAX_ROWS` and its trailing comment block, add:

```python
# Per statement, every arm. A harness property like MAX_ROWS: the frozen
# contracts declare no `resources`, and adding one would change their
# published digests. 120 s is far past any query a DABStep answer needs; it
# exists to end a runaway join, which in one unattended sweep pinned a CPU
# for 35 minutes (#116).
HARNESS_QUERY_SECONDS = 120

# Per worker: each worker has its own working copy, so its own DuckDB
# instance. Four workers at 512MB leave headroom under the 4 GiB container
# that was OOM-killed without it (#116).
HARNESS_MEMORY_LIMIT = "512MB"

_TRUNCATION_MARKER = "-- truncated at {n} rows (more rows not shown)"


class _BoundedDuckDBAdapter(DuckDBAdapter):
    """Every arm's queries run through this adapter, so every arm is bounded
    by the same code: `MAX_ROWS` via `execute_limited`, `HARNESS_QUERY_SECONDS`
    when no contract limit applies, and `HARNESS_MEMORY_LIMIT`."""

    def __init__(self, db_path: Path) -> None:
        super().__init__(str(db_path), memory_limit=HARNESS_MEMORY_LIMIT)

    def execute_limited(
        self, sql: str, max_rows: int, timeout_seconds: float | None = None
    ) -> QueryResult:
        if timeout_seconds is None:
            timeout_seconds = HARNESS_QUERY_SECONDS
        return super().execute_limited(sql, max_rows, timeout_seconds)
```

(Import `QueryResult` from `agentic_data_contracts.adapters.base`.)

4. Replace `execute_sql`'s body:

```python
    def execute_sql(sql: str) -> str:
        """Run a SQL query and return its result as CSV (header + rows)."""
        adapter = None
        try:
            adapter = _BoundedDuckDBAdapter(db_path)
            result = adapter.execute_limited(sql, MAX_ROWS)
            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow(result.columns)
            writer.writerows(result.rows)
            text = buf.getvalue().rstrip("\n")
            if result.truncated:
                text += "\n" + _TRUNCATION_MARKER.format(n=MAX_ROWS)
            return text
        except Exception as exc:  # surfaced to the model, same as arm C's errors
            return f"ERROR: {exc}"
        finally:
            if adapter is not None:
                adapter.connection.close()
```

Keep `list_tables` and `describe_table` as they are.

5. Rename `_truncate_run_query(tool_def, max_rows)` to `_append_truncation_marker(tool_def, max_rows)`. Keep its payload-locating and parse-or-raise logic. Replace the section from `rows = data.get("rows")` to the `return {**result, ...}` with:

```python
        if data.get("truncated") is not True:
            return result
        marker = _TRUNCATION_MARKER.format(n=max_rows)
        return {**result, "content": [{"type": "text", "text": f"{text}\n{marker}"}]}
```

Rewrite its docstring's first paragraph: "Append the marker `execute_sql` appends when `run_query` reports `truncated`. The library caps the rows (`create_tools(max_result_rows=MAX_ROWS)`); this only makes the governed arms read the same marker text as the ungoverned ones." Keep the paragraph about preambles, blocked responses and parse-or-raise.

6. In `_governed_tools`, use `adapter = _BoundedDuckDBAdapter(db_path)` and `create_tools(contract, adapter=adapter, session=session, max_result_rows=MAX_ROWS)`, and wrap only `run_query`:

```python
    tool_defs = [
        _append_truncation_marker(t, MAX_ROWS) if t.name == "run_query" else t
        for t in tool_defs
    ]
```

Replace the comment above it with one saying `preview_table` needs no wrapping, because `max_result_rows` caps its `LIMIT` at `MAX_ROWS`.

- [ ] **Step 4: Run the harness suite**

Run: `cd experiments/dabstep-contract-eval && uv run pytest -q`
Expected: all PASS. The frozen-digest tests must still pass unchanged; if one fails, stop, because a contract file was touched.

- [ ] **Step 5: Commit**

```bash
git add experiments/dabstep-contract-eval/dce/arms.py experiments/dabstep-contract-eval/tests/test_arms.py
git commit -m "fix(dabstep): same row, time and memory bounds on every arm; stop draining to count (#116)"
```

---

### Task 6: Docs, changelog, version, full verification

**Files:**
- Modify: `README.md` (the adapter-capabilities section next to `TimeoutAdapter`, and the `create_tools` arguments)
- Modify: `docs/architecture.md` (the resource-limits bullets, next to "Query time")
- Modify: `CHANGELOG.md`, `pyproject.toml` (version `0.55.0`), `uv.lock` (self version)

- [ ] **Step 1: Write the docs**

README, next to the `TimeoutAdapter` paragraph: document `RowLimitAdapter.execute_limited(sql, max_rows, timeout_seconds=None)`:
- It fetches at most `max_rows + 1` rows and sets `truncated`.
- `timeout_seconds` is passed only to an adapter that is also a `TimeoutAdapter`.
- A DB-API adapter implements it with `cursor.fetchmany(max_rows + 1)`.
- Without it, results are capped for the agent only, and a warning is logged.
- A subclass of `DuckDBAdapter` that rewrites SQL in `execute` must override `execute_limited` too.

Document `DuckDBAdapter(memory_limit="512MB")` and why both limits are needed: it bounds the engine, while the row cap bounds the Python objects.

Where `create_tools` arguments are listed, add `max_result_rows` (default 1000, `None` for no cap), the `"truncated": true` payload key, and the rule that result checks see `max(cap, largest min_rows/max_rows + 1)` rows.

`docs/architecture.md`: add a **Result size** bullet after **Query time**, in the same style, covering the cap, the widening for result checks, and the fallback.

- [ ] **Step 2: Write the CHANGELOG entry**

Add `## [0.55.0] - <release date>` above 0.54.0, in 0.54.0's style:
- **Changed:** `run_query` and `preview_table` now return at most 1000 rows by default. A truncated `run_query` result carries `"truncated": true`, and `row_count` then counts the returned rows. Pass `max_result_rows=None` for 0.54.0's behaviour. The same argument is on `create_tools` and all four framework builders. Say plainly that this changes behaviour.
- **Added:**
  - `RowLimitAdapter`, with the reason it is optional.
  - `QueryResult.truncated`.
  - `DuckDBAdapter(memory_limit=...)`.
  - `DuckDBAdapter.execute_limited`.
- **Fixed:** results were materialised in full before anything cut them down, which exhausted memory and stalled other sessions on the GIL (#116). Mention that result checks stay exact.
- **Internal:** the DABStep harness gives all four arms the same bounds. Its truncation marker no longer reports a total, which applies to future runs only.

Set `version = "0.55.0"` in `pyproject.toml`, then run `uv lock` (only the self version should change; check with `git diff uv.lock`). Run the same in `experiments/dabstep-contract-eval` only if its lock records the self version, as 0.54.0 did.

- [ ] **Step 3: Full verification**

```bash
uv run pytest -q
(cd experiments/dabstep-contract-eval && uv run pytest -q)
uv run --isolated --resolution lowest-direct --extra duckdb --extra agent-sdk --extra agent-contracts --extra langchain --extra pydantic-ai --extra dev pytest -q
git add -A && prek run --all-files
```

Expected: every suite passes, including the lowest-floor run (DuckDB 1.1.1: `fetchmany` streaming, `SET memory_limit = ?` and `OutOfMemoryException` were all verified on that floor during planning). prek is clean.

- [ ] **Step 4: Commit**

```bash
git add README.md docs/architecture.md CHANGELOG.md pyproject.toml uv.lock
git commit -m "docs: max_result_rows, RowLimitAdapter, DuckDB memory_limit; 0.55.0 (#116)"
```
