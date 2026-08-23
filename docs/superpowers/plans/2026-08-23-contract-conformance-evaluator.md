# Contract Conformance Evaluator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a third validation pass over the verified-examples corpus that measures whether an agent can reproduce a certified answer from the contract alone, using the governed tool path.

**Architecture:** A `ToolRecorder` rides on `ContractSession` — which every tool entry point already accepts — so one implementation instruments all four framework wrappers with no public signature changes. The library never runs an agent: the consumer's loop produces `Attempt` records, and `evaluate_conformance` is a pure synchronous function over them, scoring two orthogonal axes (answer, protocol).

**Tech Stack:** Python 3.12+, sqlglot, pydantic, pyyaml, thefuzz. No new dependency. `uv` for execution, `prek` for lint/format/type.

**Spec:** `docs/superpowers/specs/2026-08-23-contract-conformance-evaluator-design.md`

## Global Constraints

- **No new dependency.** The base install must remain `sqlglot>=28.6`, `pydantic>=2.11`, `pyyaml>=6.0.3`, `thefuzz>=0.22.1`. Nothing in this plan may add one.
- **Every change is additive and default-off.** `recorder=None` must produce byte-identical behavior to today for existing users.
- **Run everything through `uv run`.** e.g. `uv run pytest tests/test_validation/test_conformance.py -v`.
- **Lint via prek, never bare tools.** `prek run --all-files` reproduces CI. Never invoke `ruff` or `ty` directly — the hook `rev`s in `.pre-commit-config.yaml` are the pinned versions.
- **TDD is mandatory.** Every task writes a failing test first and runs it to confirm it fails before implementing.
- **Never reference `file.py:NNN` in committed code or docs** — a pre-commit hook rejects it. Name the symbol instead.
- **Commit after every task.** Small commits, conventional-commit prefixes (`feat:`, `refactor:`, `test:`, `docs:`).
- **The library never calls a model.** No task may import an LLM client or make a network call.
- **Test fixtures are module-local, not shared.** The suite has exactly one shared fixture (`fixtures_dir` in `tests/conftest.py`). Any task whose tests need a contract, adapter, or semantic source defines them at the top of its own test file, following `tests/test_tools/test_factory.py`:

  ```python
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
              id INTEGER, amount DECIMAL(10,2), tenant_id VARCHAR, created_at DATE
          );
          INSERT INTO analytics.orders VALUES
              (1, 100.00, 'acme', DATE '2026-01-05'),
              (2, 200.00, 'acme', DATE '2026-01-06');
          """
      )
      return db

  @pytest.fixture
  def semantic(fixtures_dir: Path) -> YamlSource:
      return YamlSource(fixtures_dir / "semantic_source.yml")
  ```

  Facts that follow from these fixtures and must not be re-guessed:
  - `create_tools(contract, adapter=adapter, semantic_source=semantic)` — the semantic source is passed **explicitly**. `valid_contract.yml` points its source at a dbt manifest path that does not resolve, so auto-loading yields "No semantic source configured."
  - `SELECT COUNT(id) FROM analytics.orders WHERE tenant_id = 'acme'` returns **2**.
  - `valid_contract.yml` blocks `SELECT *` and requires a `tenant_id` filter. Every query meant to succeed must name explicit columns and filter on `tenant_id`.
  - The metrics available are `total_revenue` and `active_customers`.
  - `created_at` is added above specifically so the relative-time path can be exercised; the repo's other test files omit it.
- **`lookup_metric` reads its argument as `metric_name`, not `name`.** Every call and every `args.get(...)` uses `metric_name`. With `name`, the consulted-metrics set is always empty and the protocol rule reports a violation for every compliant agent.
- **`lookup_metric`'s two non-exact paths return `_text_response`, not `_error_response`** — fuzzy candidates return JSON with `exact_match: false`, and no-match returns the plain string `Metric '<x>' not found.` Both must be recorded as `outcome="miss"` at their own return sites; neither carries a `_kind` to read.

---

### Task 1: The recorder types

**Files:**
- Create: `src/agentic_data_contracts/core/recorder.py`
- Test: `tests/test_core/test_recorder.py`

**Interfaces:**
- Consumes: nothing (standard library only — this is deliberate, see below).
- Produces: `ToolCall` (frozen dataclass), `ToolRecorder` with `.calls: list[ToolCall]`, `.log(...)`, `.elapsed_seconds: float`, `.consume() -> list[ToolCall]`.

This module depends on nothing but the standard library so that `core/session.py` can import it at runtime. Putting these types in `validation/conformance.py` would create a hard cycle: `core.session` → `validation.conformance` → `validation/__init__` → `examples.py` → `adapters` → `core.session`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_core/test_recorder.py
import pytest

from agentic_data_contracts.core.recorder import ToolCall, ToolRecorder


def test_log_appends_a_call_with_an_incrementing_sequence():
    rec = ToolRecorder()
    rec.log("lookup_metric", {"name": "CAC"}, "ok")
    rec.log("run_query", {"sql": "SELECT 1"}, "ok", scalar=1.0, row_count=1)

    assert [c.sequence for c in rec.calls] == [0, 1]
    assert rec.calls[0] == ToolCall(
        sequence=0, tool="lookup_metric", args={"name": "CAC"}, outcome="ok"
    )
    assert rec.calls[1].scalar == 1.0
    assert rec.calls[1].row_count == 1


def test_unknown_outcome_is_rejected_at_log_time():
    rec = ToolRecorder()
    with pytest.raises(ValueError, match="outcome must be one of"):
        rec.log("run_query", {}, "sort-of-ok")


def test_elapsed_seconds_is_measured_from_construction():
    rec = ToolRecorder()
    assert rec.elapsed_seconds >= 0.0


def test_consume_returns_the_calls_and_refuses_a_second_read():
    rec = ToolRecorder()
    rec.log("run_query", {}, "ok")

    assert len(rec.consume()) == 1
    with pytest.raises(ValueError, match="already consumed"):
        rec.consume()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_core/test_recorder.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'agentic_data_contracts.core.recorder'`

- [ ] **Step 3: Write minimal implementation**

```python
# src/agentic_data_contracts/core/recorder.py
"""Records which contract tools an agent actually called during one run.

Standard library only, and deliberately so: ``core.session`` imports this at
runtime, and anything reaching into ``validation`` from here would close the
cycle ``core.session -> validation.conformance -> validation/__init__ ->
examples -> adapters -> core.session``.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

OUTCOMES = frozenset({"ok", "miss", "blocked", "error"})


@dataclass(frozen=True)
class ToolCall:
    """One recorded contract-tool call.

    ``outcome`` is the load-bearing field. ``ok`` means the call resolved
    exactly; ``miss`` means a lookup did not (fuzzy fallback fired, or nothing
    matched); ``blocked`` means governance rejected the SQL; ``error`` means the
    tool raised *or returned an error payload without raising*.
    """

    sequence: int
    tool: str
    args: dict[str, Any] = field(default_factory=dict)
    outcome: str = "ok"
    detail: str | None = None
    scalar: float | None = None
    row_count: int | None = None
    relative_time: str | None = None


class ToolRecorder:
    """Collects ``ToolCall`` records for a single agent attempt.

    Times itself from construction rather than reading
    ``ContractSession.elapsed_seconds``: the session timer starts in
    ``_ensure_timer``, reached only from ``check_limits``, which only
    ``run_query`` calls -- so a session-derived duration reads 0.0 for exactly
    the attempts worth timing, the ones where the agent never reached a
    successful query.
    """

    def __init__(self) -> None:
        self.calls: list[ToolCall] = []
        self._start = time.monotonic()
        self._consumed = False

    @property
    def elapsed_seconds(self) -> float:
        return time.monotonic() - self._start

    def log(
        self,
        tool: str,
        args: dict[str, Any],
        outcome: str,
        *,
        detail: str | None = None,
        scalar: float | None = None,
        row_count: int | None = None,
        relative_time: str | None = None,
    ) -> None:
        if outcome not in OUTCOMES:
            raise ValueError(
                f"outcome must be one of {sorted(OUTCOMES)}, got {outcome!r}"
            )
        self.calls.append(
            ToolCall(
                sequence=len(self.calls),
                tool=tool,
                args=dict(args),
                outcome=outcome,
                detail=detail,
                scalar=scalar,
                row_count=row_count,
                relative_time=relative_time,
            )
        )

    def consume(self) -> list[ToolCall]:
        """Hand over the call log exactly once.

        A recorder reused across two questions would merge their call logs and
        silently produce wrong protocol verdicts. Raising here follows the same
        rule as ``VerifiedExample.__post_init__``: a loud error beats a quiet
        wrong answer.
        """
        if self._consumed:
            raise ValueError("ToolRecorder already consumed — use one per attempt")
        self._consumed = True
        return list(self.calls)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_core/test_recorder.py -v`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/core/recorder.py tests/test_core/test_recorder.py
git commit -m "feat: ToolCall and ToolRecorder for conformance evaluation"
```

---

### Task 2: Attach the recorder to ContractSession

**Files:**
- Modify: `src/agentic_data_contracts/core/session.py` (the `ContractSession.__init__` method)
- Test: `tests/test_core/test_session.py`

**Interfaces:**
- Consumes: `ToolRecorder` from Task 1.
- Produces: `ContractSession(contract, recorder=None)` with a `.recorder: ToolRecorder | None` attribute.

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_core/test_session.py
from agentic_data_contracts.core.recorder import ToolRecorder


def test_session_defaults_to_no_recorder(contract):
    assert ContractSession(contract).recorder is None


def test_session_accepts_a_recorder(contract):
    rec = ToolRecorder()
    assert ContractSession(contract, recorder=rec).recorder is rec
```

Use whatever contract fixture `tests/test_core/test_session.py` already uses; if it constructs a contract inline, do the same rather than adding a fixture.

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_core/test_session.py -k recorder -v`
Expected: FAIL with `TypeError: ContractSession.__init__() got an unexpected keyword argument 'recorder'`

- [ ] **Step 3: Write minimal implementation**

In `core/session.py`, add the import and extend `__init__`:

```python
from agentic_data_contracts.core.recorder import ToolRecorder


class ContractSession:
    """Tracks enforcement state for a single agent run."""

    def __init__(
        self, contract: DataContract, *, recorder: ToolRecorder | None = None
    ) -> None:
        self.contract = contract
        self.recorder = recorder
        self.retries: int = 0
        ...  # leave the remaining existing lines untouched
```

`recorder` is keyword-only so no positional caller can be broken by the new parameter.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_core/ -v`
Expected: PASS, including every pre-existing session test

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/core/session.py tests/test_core/test_session.py
git commit -m "feat: ContractSession accepts an optional ToolRecorder"
```

---

### Task 3: Split `_scalar` into a pure rule and an executing wrapper

**Files:**
- Modify: `src/agentic_data_contracts/validation/_scalar.py`
- Test: `tests/test_validation/test_scalar_value.py` (create)

**Interfaces:**
- Produces: `_scalar_value(columns: list[str], rows: list[Any], label: str) -> tuple[float | None, str | None]`. `_scalar(adapter, sql, label)` keeps its exact current signature and behavior.

Required, not cosmetic: today `_scalar` executes the query itself, so the recorder inside `run_query` cannot reuse it without re-running every query the agent already ran.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_validation/test_scalar_value.py
import pytest

from agentic_data_contracts.validation._scalar import _scalar_value


def test_returns_the_value_for_a_scalar_shaped_result():
    assert _scalar_value(["total"], [(42,)], "metric") == (42.0, None)


def test_reports_each_unusable_condition_distinctly():
    assert _scalar_value(["total"], [], "metric") == (None, "metric returned no rows")
    assert _scalar_value(["total"], [(None,)], "metric") == (
        None,
        "metric returned NULL",
    )
    value, reason = _scalar_value(["total"], [(float("nan"),)], "metric")
    assert value is None
    assert "non-finite" in reason


def test_raises_when_not_scalar_shaped():
    with pytest.raises(ValueError, match="exactly one column"):
        _scalar_value(["a", "b"], [(1, 2)], "metric")
    with pytest.raises(ValueError, match="at most one row"):
        _scalar_value(["a"], [(1,), (2,)], "metric")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_validation/test_scalar_value.py -v`
Expected: FAIL with `ImportError: cannot import name '_scalar_value'`

- [ ] **Step 3: Write minimal implementation**

Rewrite the body of `validation/_scalar.py`, moving every existing rule into the pure function and leaving `_scalar` as a two-line wrapper. Keep the module docstring; extend it to say the rule is now shared with the tool recorder.

```python
def _scalar_value(
    columns: list[str], rows: list[Any], label: str
) -> tuple[float | None, str | None]:
    """Apply the scalar rule to an already-fetched result.

    Split out of ``_scalar`` so callers holding a result -- the ``run_query``
    tool recorder -- get identical empty / NULL / non-finite / non-scalar
    semantics without paying for a second execution of the query.
    """
    if len(columns) != 1:
        raise ValueError(
            f"{label} query must return exactly one column, got {len(columns)}"
        )
    if len(rows) > 1:
        raise ValueError(f"{label} query must return at most one row, got {len(rows)}")
    if not rows:
        return None, f"{label} returned no rows"
    value = rows[0][0]
    if value is None:
        return None, f"{label} returned NULL"
    number = float(value)
    if not math.isfinite(number):
        return None, f"{label} returned a non-finite value: {number}"
    return number, None


def _scalar(
    adapter: DatabaseAdapter, sql: str, label: str
) -> tuple[float | None, str | None]:
    """Measure ``sql`` as a single scalar.

    Returns ``(value, None)`` for a usable finite number, or ``(None, reason)``
    when the query yields no usable value. Raises ``ValueError`` if the query is
    not scalar-shaped.
    """
    result = adapter.execute(sql)
    return _scalar_value(result.columns, result.rows, label)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_validation/ -v`
Expected: PASS — the new file plus every existing `check_example_answers` and `reconcile_decomposition` test, which exercise `_scalar` unchanged

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/validation/_scalar.py tests/test_validation/test_scalar_value.py
git commit -m "refactor: split the scalar rule out of its executing wrapper"
```

---

### Task 4: Extract the relative-time helpers to a shared module

**Files:**
- Create: `src/agentic_data_contracts/validation/_timewindow.py`
- Modify: `src/agentic_data_contracts/validation/examples.py` (delete the two functions, import them instead)
- Test: `tests/test_validation/test_timewindow.py` (create)

**Interfaces:**
- Produces: `_relative_time_node(statement: exp.Expression) -> str | None` and `_is_clock_read(call: exp.Anonymous) -> bool`, importable from `validation/_timewindow.py`.

Task 5 needs `_relative_time_node` inside `validator.py`. It currently lives in `examples.py`, which imports `Validator` — so importing it back would be circular. Extraction first, same pattern the codebase already uses for `_scalar.py`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_validation/test_timewindow.py
import sqlglot

from agentic_data_contracts.validation._timewindow import _relative_time_node


def test_names_a_relative_time_function():
    stmt = sqlglot.parse_one("SELECT 1 WHERE d > CURRENT_DATE - 7")
    assert _relative_time_node(stmt) is not None


def test_returns_none_for_a_pinned_window():
    stmt = sqlglot.parse_one("SELECT 1 WHERE d BETWEEN '2026-01-01' AND '2026-01-31'")
    assert _relative_time_node(stmt) is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_validation/test_timewindow.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'agentic_data_contracts.validation._timewindow'`

- [ ] **Step 3: Write minimal implementation**

Create `validation/_timewindow.py` with a module docstring explaining the split, then **move** `_relative_time_node` and `_is_clock_read` into it verbatim from `examples.py` — including the module-level tuple of sqlglot node types that the sqlglot floor comment in `pyproject.toml` refers to. Move that tuple too; it must not be duplicated.

In `examples.py`, delete both function bodies and the tuple, and add:

```python
from agentic_data_contracts.validation._timewindow import _relative_time_node
```

Import `_is_clock_read` as well only if `examples.py` still references it directly; if its only caller was `_relative_time_node`, it moves and is not re-imported.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_validation/ -v`
Expected: PASS — new tests plus every existing `check_example_answers` relative-time test, unchanged

- [ ] **Step 5: Update the sqlglot floor comment**

`pyproject.toml`'s `sqlglot>=28.6` comment names `validation/examples.py` as the location of the relative-time scan. Update that sentence to name `validation/_timewindow.py`. Do not change the floor itself.

- [ ] **Step 6: Commit**

```bash
git add src/agentic_data_contracts/validation/_timewindow.py src/agentic_data_contracts/validation/examples.py tests/test_validation/test_timewindow.py pyproject.toml
git commit -m "refactor: relative-time helpers move to a shared private module"
```

---

### Task 5: Report the relative-time node on ValidationResult

**Files:**
- Modify: `src/agentic_data_contracts/validation/validator.py` (the `ValidationResult` dataclass and the `validate` method)
- Test: `tests/test_validation/test_validator_relative_time.py` (create)

**Interfaces:**
- Consumes: `_relative_time_node` from Task 4.
- Produces: `ValidationResult.relative_time: str | None`.

`run_query` must record whether the agent's SQL used a relative time window, but `evaluate_conformance` is pure and has no dialect or normalizer. The validator already parses the statement under the right dialect, so it reports the fact and the recorder reads it. This keeps the verdict layer free of re-parsing.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_validation/test_validator_relative_time.py
from agentic_data_contracts.validation.validator import Validator


def test_validate_names_a_relative_time_node(contract):
    result = Validator(contract).validate(
        "SELECT amount FROM analytics.orders WHERE created_at > CURRENT_DATE - 7"
    )
    assert result.relative_time is not None


def test_validate_reports_none_for_a_pinned_window(contract):
    result = Validator(contract).validate(
        "SELECT amount FROM analytics.orders WHERE created_at > '2026-01-01'"
    )
    assert result.relative_time is None


def test_unparseable_sql_leaves_relative_time_none(contract):
    result = Validator(contract).validate("NOT SQL AT ALL ((")
    assert result.parse_error is True
    assert result.relative_time is None
```

Reuse the contract fixture `tests/test_validation/test_validator.py` already uses. The queries must reference a table that fixture allows, or the result is blocked before parsing matters — check the fixture and adjust the table name.

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_validation/test_validator_relative_time.py -v`
Expected: FAIL with `AttributeError: 'ValidationResult' object has no attribute 'relative_time'`

- [ ] **Step 3: Write minimal implementation**

Add the field to `ValidationResult`, after `parse_error` so no positional construction breaks:

```python
    parse_error: bool = False
    relative_time: str | None = None
```

In `validate()`, immediately after the successful `sqlglot.parse_one(...)` call that produces `statement`, compute the value and pass it into every `ValidationResult` returned from that point on:

```python
        relative_time = _relative_time_node(statement)
```

Add `relative_time=relative_time` to the `ValidationResult(...)` constructions in `validate()` that are reached *after* the parse. The early return that sets `parse_error=True` leaves it at its `None` default — correct, because unparseable SQL (the Denodo/VQL path) cannot be scanned.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_validation/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/validation/validator.py tests/test_validation/test_validator_relative_time.py
git commit -m "feat: ValidationResult reports the query's relative-time node"
```

---

### Task 6: Give error responses a machine-readable kind

**Files:**
- Modify: `src/agentic_data_contracts/tools/factory.py` (the `_error_response` helper and all of its call sites)
- Test: `tests/test_tools/test_error_response_kind.py` (create)

**Interfaces:**
- Produces: `_error_response(text: str, kind: str = "error") -> dict[str, Any]`, whose returned dict carries `_kind` alongside the existing `is_error`.

Without this, the recorder can only classify outcome by "did it raise". The tools return errors far more often than they raise them, so every `_error_response("No semantic source configured.")` would log as a successful metric consultation — and P2 in Task 13 would certify `followed` for an agent that never saw a metric definition.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_tools/test_error_response_kind.py
from agentic_data_contracts.tools.factory import _error_response


def test_defaults_to_error_kind():
    assert _error_response("boom")["_kind"] == "error"


def test_blocked_kind_is_carried():
    assert _error_response("BLOCKED — nope", kind="blocked")["_kind"] == "blocked"


def test_is_error_is_still_set_for_mcp():
    assert _error_response("boom")["is_error"] is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_tools/test_error_response_kind.py -v`
Expected: FAIL with `KeyError: '_kind'`

- [ ] **Step 3: Write minimal implementation**

Extend the helper, keeping its existing docstring and adding a paragraph on `_kind`:

```python
def _error_response(text: str, kind: str = "error") -> dict[str, Any]:
    response = _text_response(text)
    response["is_error"] = True
    response["_kind"] = kind
    return response
```

Match the existing body — read it first and preserve exactly how `is_error` and the content envelope are currently constructed; only add `_kind`.

Then pass `kind="blocked"` at every call site whose message denies a *governance* decision — the ones whose text begins `BLOCKED`. Leave misconfiguration and invalid-argument sites on the default `"error"`. There are eighteen `_error_response` call sites; the `BLOCKED` ones are in `run_query` (session limit exceeded, validation blocked, execution failed, result check blocked) and in `inspect_query`'s equivalent paths. Read each site's message before deciding.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_tools/ -v`
Expected: PASS — including every existing tool test, since `_kind` is additive

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/tools/factory.py tests/test_tools/test_error_response_kind.py
git commit -m "feat: error responses carry a machine-readable kind"
```

---

### Task 7: Instrument the seven lookup and inspection tools

**Files:**
- Modify: `src/agentic_data_contracts/tools/factory.py` (the closures for `describe_table`, `preview_table`, `list_metrics`, `lookup_metric`, `lookup_domain`, `lookup_relationships`, `trace_metric_impacts`, `inspect_query`)
- Test: `tests/test_tools/test_recorder_integration.py` (create)

**Interfaces:**
- Consumes: `ContractSession.recorder` from Task 2; `_error_response`'s `_kind` from Task 6.
- Produces: recorded `ToolCall`s for every non-`run_query` tool.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_tools/test_recorder_integration.py
import pytest

from agentic_data_contracts.core.recorder import ToolRecorder
from agentic_data_contracts.core.session import ContractSession
from agentic_data_contracts.tools.factory import create_tools


def _tools(contract, recorder, adapter=None):
    session = ContractSession(contract, recorder=recorder)
    return {t.name: t.callable for t in create_tools(contract, adapter=adapter, session=session)}


@pytest.mark.asyncio
async def test_exact_metric_lookup_records_ok(contract):
    rec = ToolRecorder()
    tools = _tools(contract, rec)
    await tools["lookup_metric"]({"metric_name": "total_revenue"})

    assert [(c.tool, c.outcome) for c in rec.calls] == [("lookup_metric", "ok")]


@pytest.mark.asyncio
async def test_fuzzy_metric_lookup_records_miss(contract):
    rec = ToolRecorder()
    tools = _tools(contract, rec)
    await tools["lookup_metric"]({"metric_name": "revenu"})

    assert rec.calls[0].outcome == "miss"


@pytest.mark.asyncio
async def test_no_recorder_records_nothing_and_still_works(contract):
    session = ContractSession(contract)
    tools = {t.name: t.callable for t in create_tools(contract, session=session)}
    result = await tools["lookup_metric"]({"metric_name": "total_revenue"})

    assert session.recorder is None
    assert result["content"]


@pytest.mark.asyncio
async def test_every_tool_records_at_least_one_call(contract, adapter):
    """A wholly uninstrumented new tool must fail CI."""
    for name in [
        "describe_table", "preview_table", "list_metrics", "lookup_metric",
        "lookup_domain", "lookup_relationships", "trace_metric_impacts",
        "inspect_query", "run_query",
    ]:
        rec = ToolRecorder()
        tools = _tools(contract, rec, adapter=adapter)
        await tools[name](_MINIMAL_ARGS[name])
        assert rec.calls, f"{name} recorded nothing"
        assert rec.calls[0].tool == name
```

Define `_MINIMAL_ARGS` as a module-level dict mapping each tool name to the smallest valid argument dict for the fixture contract. Reuse the contract and DuckDB adapter fixtures that `tests/test_tools/` already provides — read `tests/conftest.py` and the existing tool tests before writing, and use their fixture names rather than inventing new ones.

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_tools/test_recorder_integration.py -v`
Expected: FAIL — `rec.calls` is empty; `assert rec.calls` fails on the first tool

- [ ] **Step 3: Write minimal implementation**

In each of the eight closures, add a guarded log immediately before every `return`:

```python
        if session.recorder is not None:
            session.recorder.log("lookup_metric", args, "ok")
```

Choose the outcome per return path:
- an exact hit → `"ok"`
- a fuzzy-fallback or not-found response → `"miss"`, with the candidate names in `detail`
- a returned `_error_response` → its `_kind` (`"blocked"` or `"error"`)
- a raised exception → `"error"` with `str(e)` in `detail`, logged before re-raising

The not-found case matters and is easy to get wrong: `_error_response`'s own docstring says a lookup that legitimately found nothing is *not* an error response — it returns normal text. So that path must be logged `"miss"` explicitly at its return site; there is no `_kind` to read.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_tools/ -v`
Expected: PASS. The `test_every_tool_records_at_least_one_call` case will still fail on `run_query` — that is Task 8. Mark it `@pytest.mark.xfail(strict=True, reason="run_query instrumented in the next task")` for this commit and remove the marker in Task 8.

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/tools/factory.py tests/test_tools/test_recorder_integration.py
git commit -m "feat: record lookup and inspection tool calls"
```

---

### Task 8: Instrument run_query's six return paths

**Files:**
- Modify: `src/agentic_data_contracts/tools/factory.py` (the `run_query` closure)
- Modify: `tests/test_tools/test_recorder_integration.py` (remove the xfail marker, add the per-path test)

**Interfaces:**
- Consumes: `_scalar_value` from Task 3; `ValidationResult.relative_time` from Task 5; `_kind` from Task 6.
- Produces: `ToolCall`s for `run_query` carrying `scalar`, `row_count`, and `relative_time`.

`run_query` gets its own task because it has six return paths and the per-tool test from Task 7 passes on the success path alone. A dropped `blocked` log line would ship green and silently turn `violated` rows into `followed` ones.

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_tools/test_recorder_integration.py
@pytest.mark.asyncio
async def test_successful_query_records_scalar_and_row_count(contract, adapter):
    rec = ToolRecorder()
    tools = _tools(contract, rec, adapter=adapter)
    await tools["run_query"]({"sql": "SELECT COUNT(id) FROM analytics.orders WHERE tenant_id = 'acme'"})

    call = rec.calls[-1]
    assert call.outcome == "ok"
    assert call.scalar is not None
    assert call.row_count == 1


@pytest.mark.asyncio
async def test_multi_row_result_records_no_scalar_but_is_still_ok(contract, adapter):
    """A table-shaped answer is ordinary for run_query, not a failure."""
    rec = ToolRecorder()
    tools = _tools(contract, rec, adapter=adapter)
    await tools["run_query"]({"sql": "SELECT order_id, amount FROM analytics.orders WHERE tenant_id = 'acme'"})

    call = rec.calls[-1]
    assert call.outcome == "ok"
    assert call.scalar is None


@pytest.mark.asyncio
async def test_blocked_query_records_blocked(contract, adapter):
    rec = ToolRecorder()
    tools = _tools(contract, rec, adapter=adapter)
    await tools["run_query"]({"sql": "SELECT * FROM analytics.orders"})

    assert rec.calls[-1].outcome == "blocked"
    assert rec.calls[-1].detail


@pytest.mark.asyncio
async def test_missing_adapter_records_error_not_ok(contract):
    """The regression that would certify a governed path that never ran."""
    rec = ToolRecorder()
    tools = _tools(contract, rec, adapter=None)
    await tools["run_query"]({"sql": "SELECT amount FROM analytics.orders WHERE tenant_id = 'acme'"})

    assert rec.calls[-1].outcome == "error"


@pytest.mark.asyncio
async def test_relative_time_window_is_recorded(contract, adapter):
    rec = ToolRecorder()
    tools = _tools(contract, rec, adapter=adapter)
    await tools["run_query"]({
        "sql": "SELECT amount FROM analytics.orders WHERE tenant_id = 'acme' AND created_at > CURRENT_DATE - 7"
    })

    assert rec.calls[-1].relative_time is not None
```

Adjust every SQL string to satisfy the fixture contract's rules (allowed tables, required filters). Read the existing `run_query` tests first and copy their known-good and known-blocked queries.

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_tools/test_recorder_integration.py -v`
Expected: FAIL — `IndexError` on `rec.calls[-1]`, since `run_query` records nothing yet

- [ ] **Step 3: Write minimal implementation**

Add a guarded log before each of the six returns in `run_query`. Extract a local helper at the top of the closure so the six sites stay short:

```python
        def _record(
            outcome: str,
            *,
            detail: str | None = None,
            scalar: float | None = None,
            row_count: int | None = None,
            relative_time: str | None = None,
        ) -> None:
            if session.recorder is not None:
                session.recorder.log(
                    "run_query",
                    args,
                    outcome,
                    detail=detail,
                    scalar=scalar,
                    row_count=row_count,
                    relative_time=relative_time,
                )
```

Then, path by path:

1. Session limit exceeded → `_record("blocked", detail=str(e))`
2. Validation blocked → `_record("blocked", detail="; ".join(vresult.reasons), relative_time=vresult.relative_time)`
3. No adapter → `_record("error", detail="no database adapter configured")`
4. Execution raised → `_record("error", detail=str(e), relative_time=vresult.relative_time)`
5. Result check blocked → `_record("blocked", detail="; ".join(rresult.reasons), relative_time=vresult.relative_time)`
6. Success → compute the scalar from the result already in hand, then record:

```python
        try:
            scalar, _ = _scalar_value(qresult.columns, qresult.rows, "run_query")
        except ValueError:
            # Not scalar-shaped. Ordinary for run_query -- a table is a
            # legitimate answer -- so it records as "no scalar", never an error.
            scalar = None
        _record(
            "ok",
            scalar=scalar,
            row_count=qresult.row_count,
            relative_time=vresult.relative_time,
        )
```

Import `_scalar_value` at the top of `factory.py` from `agentic_data_contracts.validation._scalar`. `tools` already imports from `validation` (it uses `Validator`), so this adds no new dependency direction.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_tools/ -v`
Expected: PASS. Remove the `xfail` marker added in Task 7 and confirm `test_every_tool_records_at_least_one_call` now passes for all nine tools.

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/tools/factory.py tests/test_tools/test_recorder_integration.py
git commit -m "feat: record all six run_query return paths"
```

---

### Task 9: Add expects_metrics to VerifiedExample

**Files:**
- Modify: `src/agentic_data_contracts/validation/examples.py` (`_KNOWN_KEYS`, the `VerifiedExample` dataclass, `__post_init__`, `from_dict`)
- Test: `tests/test_validation/test_examples.py`

**Interfaces:**
- Produces: `VerifiedExample.expects_metrics: list[str]`, consumed by Task 13's P2 rule.

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_validation/test_examples.py
def test_expects_metrics_defaults_to_empty():
    assert VerifiedExample(sql="SELECT 1").expects_metrics == []


def test_expects_metrics_round_trips_through_from_dict():
    ex = VerifiedExample.from_dict({"sql": "SELECT 1", "expects_metrics": ["CAC"]})
    assert ex.expects_metrics == ["CAC"]


def test_expects_metrics_is_not_duplicated_into_metadata():
    """_KNOWN_KEYS must list it, or from_dict copies it into metadata too."""
    ex = VerifiedExample.from_dict({"sql": "SELECT 1", "expects_metrics": ["CAC"]})
    assert "expects_metrics" not in ex.metadata


def test_expects_metrics_needs_no_expected():
    """A protocol-only row may declare it; it is not an orphaned assertion key."""
    ex = VerifiedExample(sql="SELECT 1", expects_metrics=["CAC"])
    assert ex.expected is None


def test_malformed_expects_metrics_is_rejected():
    with pytest.raises(ValueError, match="expects_metrics"):
        VerifiedExample(sql="SELECT 1", expects_metrics="CAC")
    with pytest.raises(ValueError, match="expects_metrics"):
        VerifiedExample(sql="SELECT 1", expects_metrics=[""])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_validation/test_examples.py -k expects_metrics -v`
Expected: FAIL with `TypeError: VerifiedExample.__init__() got an unexpected keyword argument 'expects_metrics'`

- [ ] **Step 3: Write minimal implementation**

Add `"expects_metrics"` to `_KNOWN_KEYS`. Add the field after `time_scoped`:

```python
    expects_metrics: list[str] = field(default_factory=list)
```

Extend the `VerifiedExample` docstring with a sentence: declaring `expects_metrics` makes the row activate the conformance evaluator's metric-consultation rule; it is independent of `expected`, so a protocol-only row may set it.

Validate at the top of `__post_init__`, before the existing `time_scoped` check:

```python
        if not isinstance(self.expects_metrics, list) or any(
            not isinstance(m, str) or not m.strip() for m in self.expects_metrics
        ):
            raise ValueError(
                "'expects_metrics' must be a list of non-empty metric names, "
                f"got {self.expects_metrics!r}"
            )
```

It must **not** join the orphaned-key check: that check exists because `rel_tol` / `abs_tol` / `time_scoped` only modify how an `expected` is compared, and `expects_metrics` does not.

Add `expects_metrics=raw.get("expects_metrics", [])` to the `from_dict` constructor call.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_validation/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/validation/examples.py tests/test_validation/test_examples.py
git commit -m "feat: VerifiedExample declares the metrics an agent should consult"
```

---

### Task 10: The Attempt record

**Files:**
- Create: `src/agentic_data_contracts/validation/conformance.py`
- Test: `tests/test_validation/test_conformance.py` (create)

**Interfaces:**
- Consumes: `ToolCall` / `ToolRecorder` (Task 1), `ContractSession` (Task 2), `VerifiedExample` (Task 9).
- Produces: `Attempt` dataclass and `Attempt.from_session(example, session, *, final_text="", final_answer=None, foreign_tool_calls=(), error=None) -> Attempt`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_validation/test_conformance.py
import pytest

from agentic_data_contracts.core.recorder import ToolCall, ToolRecorder
from agentic_data_contracts.core.session import ContractSession
from agentic_data_contracts.validation.conformance import Attempt
from agentic_data_contracts.validation.examples import VerifiedExample


def _example(**kw):
    return VerifiedExample(sql=kw.pop("sql", "SELECT 1"), **kw)


def test_from_session_captures_the_call_log_and_cost(contract):
    rec = ToolRecorder()
    session = ContractSession(contract, recorder=rec)
    rec.log("run_query", {"sql": "SELECT 1"}, "ok", scalar=5.0)
    session.record_cost(0.02)

    attempt = Attempt.from_session(_example(), session, final_text="five")

    assert [c.tool for c in attempt.calls] == ["run_query"]
    assert attempt.cost_usd == 0.02
    assert attempt.elapsed_seconds >= 0.0
    assert attempt.final_text == "five"


def test_from_session_coerces_foreign_tool_calls_to_a_list(contract):
    session = ContractSession(contract, recorder=ToolRecorder())
    attempt = Attempt.from_session(
        _example(), session, foreign_tool_calls=("mcp__bigquery__execute_sql",)
    )

    assert attempt.foreign_tool_calls == ["mcp__bigquery__execute_sql"]
    attempt.foreign_tool_calls.append("another")  # must not raise


def test_from_session_refuses_a_reused_recorder(contract):
    session = ContractSession(contract, recorder=ToolRecorder())
    Attempt.from_session(_example(), session)

    with pytest.raises(ValueError, match="already consumed"):
        Attempt.from_session(_example(), session)


def test_from_session_requires_a_recorder(contract):
    with pytest.raises(ValueError, match="recorder"):
        Attempt.from_session(_example(), ContractSession(contract))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_validation/test_conformance.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'agentic_data_contracts.validation.conformance'`

- [ ] **Step 3: Write minimal implementation**

```python
# src/agentic_data_contracts/validation/conformance.py
"""Pass 3 over a verified-examples corpus: can an agent reproduce the certified
answer from the contract alone, through the governed path?

``validate_examples`` asks whether certified SQL is still allowed and plannable.
``check_example_answers`` asks whether it still returns the right number. Both
check SQL a human already got right. Neither can see a contract that stays
enforceable and accurate while quietly ceasing to *teach* -- rename a metric or
trim a domain description and both stay green.

Nothing here calls a model or touches a database. The consumer runs their own
agent and hands back ``Attempt`` records; every function below is pure.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from agentic_data_contracts.core.recorder import ToolCall
from agentic_data_contracts.validation.examples import VerifiedExample

if TYPE_CHECKING:
    from agentic_data_contracts.core.session import ContractSession


@dataclass
class Attempt:
    """One agent run against one corpus question."""

    example: VerifiedExample
    calls: list[ToolCall] = field(default_factory=list)
    final_text: str = ""
    final_answer: float | None = None
    foreign_tool_calls: list[str] = field(default_factory=list)
    cost_usd: float = 0.0
    elapsed_seconds: float = 0.0
    error: str | None = None

    @classmethod
    def from_session(
        cls,
        example: VerifiedExample,
        session: ContractSession,
        *,
        final_text: str = "",
        final_answer: float | None = None,
        foreign_tool_calls: Iterable[str] = (),
        error: str | None = None,
    ) -> Attempt:
        """Snapshot one attempt off the session that served it.

        ``foreign_tool_calls`` is coerced with ``list()``: the parameter takes
        any iterable and defaults to ``()``, while the field is ``list[str]``.
        """
        if session.recorder is None:
            raise ValueError(
                "Attempt.from_session needs a session built with a recorder: "
                "ContractSession(contract, recorder=ToolRecorder())"
            )
        return cls(
            example=example,
            calls=session.recorder.consume(),
            final_text=final_text,
            final_answer=final_answer,
            foreign_tool_calls=list(foreign_tool_calls),
            cost_usd=session.cost_usd,
            elapsed_seconds=session.recorder.elapsed_seconds,
            error=error,
        )
```

`ContractSession` is imported under `TYPE_CHECKING` only. `from __future__ import annotations` keeps the annotation unevaluated, so no runtime cycle forms.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_validation/test_conformance.py -v`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/validation/conformance.py tests/test_validation/test_conformance.py
git commit -m "feat: Attempt record for conformance evaluation"
```

---

### Task 11: Answer selection and the anchor call

**Files:**
- Modify: `src/agentic_data_contracts/validation/conformance.py`
- Test: `tests/test_validation/test_conformance.py`

**Interfaces:**
- Produces: `_select_answer(attempt) -> tuple[str, float | None, int, ToolCall | None]` returning `(answer_source, actual, scalar_candidates, anchor)`.

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_validation/test_conformance.py
from agentic_data_contracts.validation.conformance import _select_answer


def _q(scalar, outcome="ok", seq=0, **kw):
    return ToolCall(sequence=seq, tool="run_query", outcome=outcome, scalar=scalar, **kw)


def test_a_single_scalar_is_not_a_guess():
    source, actual, count, anchor = _select_answer(
        Attempt(example=_example(), calls=[_q(42.0)])
    )
    assert (source, actual, count) == ("sole_scalar", 42.0, 1)
    assert anchor is not None


def test_a_declared_answer_wins_and_anchors_on_the_last_query():
    attempt = Attempt(
        example=_example(), calls=[_q(1.0, seq=0), _q(2.0, seq=1)], final_answer=9.0
    )
    source, actual, _, anchor = _select_answer(attempt)
    assert (source, actual) == ("declared", 9.0)
    assert anchor.sequence == 1


def test_reruns_of_the_same_value_stay_unambiguous():
    """A retry after a transient failure must not demote the row."""
    attempt = Attempt(
        example=_example(), calls=[_q(100.0, seq=0), _q(100.00000000000001, seq=1)]
    )
    source, actual, count, _ = _select_answer(attempt)
    assert (source, count) == ("sole_scalar", 1)
    assert actual == 100.0


def test_genuinely_different_values_are_ambiguous():
    attempt = Attempt(example=_example(), calls=[_q(10.0, seq=0), _q(4182.0, seq=1)])
    source, actual, count, _ = _select_answer(attempt)
    assert (source, actual, count) == ("last_scalar", 4182.0, 2)


def test_non_scalar_results_are_never_candidates():
    """The 'last query returned a table' outlier must not score as an error."""
    attempt = Attempt(example=_example(), calls=[_q(42.0, seq=0), _q(None, seq=1)])
    source, actual, count, _ = _select_answer(attempt)
    assert (source, actual, count) == ("sole_scalar", 42.0, 1)


def test_blocked_queries_are_never_candidates():
    attempt = Attempt(example=_example(), calls=[_q(None, outcome="blocked", seq=0)])
    source, actual, count, anchor = _select_answer(attempt)
    assert (source, actual, count, anchor) == ("none", None, 0, None)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_validation/test_conformance.py -k select_answer -v`
Expected: FAIL with `ImportError: cannot import name '_select_answer'`

- [ ] **Step 3: Write minimal implementation**

```python
import math

from agentic_data_contracts.validation.examples import (
    _DEFAULT_ABS_TOL,
    _DEFAULT_REL_TOL,
)


def _tolerances(example: VerifiedExample) -> tuple[float, float]:
    rel = _DEFAULT_REL_TOL if example.rel_tol is None else example.rel_tol
    abs_ = _DEFAULT_ABS_TOL if example.abs_tol is None else example.abs_tol
    return rel, abs_


def _select_answer(
    attempt: Attempt,
) -> tuple[str, float | None, int, ToolCall | None]:
    """Decide which number the agent answered with, and say how sure that is.

    Returns ``(answer_source, actual, scalar_candidates, anchor)``. The *anchor*
    is the call the ordering and relative-time rules measure against; it is
    defined for the ``declared`` case too, because that is the path ambiguous
    rows are steered toward.
    """
    successful = [c for c in attempt.calls if c.tool == "run_query" and c.outcome == "ok"]
    scalar_calls = [c for c in successful if c.scalar is not None]

    rel_tol, abs_tol = _tolerances(attempt.example)
    clusters: list[ToolCall] = []
    for call in scalar_calls:
        if not any(
            math.isclose(call.scalar, seen.scalar, rel_tol=rel_tol, abs_tol=abs_tol)
            for seen in clusters
        ):
            clusters.append(call)

    if attempt.final_answer is not None:
        anchor = successful[-1] if successful else None
        return "declared", attempt.final_answer, len(clusters), anchor
    if not clusters:
        return "none", None, 0, None
    if len(clusters) == 1:
        return "sole_scalar", clusters[0].scalar, 1, clusters[0]
    return "last_scalar", scalar_calls[-1].scalar, len(clusters), scalar_calls[-1]
```

Clustering is tolerance-based, not exact float equality. `math.isclose` rather than `_compare` is deliberate: these are two measurements with no privileged reference, which is the one situation `_compare`'s docstring says it is *not* for.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_validation/test_conformance.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/validation/conformance.py tests/test_validation/test_conformance.py
git commit -m "feat: answer selection with tolerance clustering and an anchor call"
```

---

### Task 12: The answer verdict

**Files:**
- Modify: `src/agentic_data_contracts/validation/conformance.py`
- Test: `tests/test_validation/test_conformance.py`

**Interfaces:**
- Consumes: `_select_answer` (Task 11), `_compare` from `validation/examples.py`.
- Produces: `_answer_verdict(attempt, source, actual, anchor) -> tuple[str, list[str], float | None, float | None]` returning `(status, reasons, abs_diff, rel_diff)`. The two diffs populate `ConformanceResult` in Task 14 so a mismatch can name the threshold it missed.

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_validation/test_conformance.py
from agentic_data_contracts.validation.conformance import _answer_verdict


def _verdict(example, calls=(), **kw):
    attempt = Attempt(example=example, calls=list(calls), **kw)
    source, actual, _, anchor = _select_answer(attempt)
    return _answer_verdict(attempt, source, actual, anchor)[0]


def test_a_crashed_attempt_is_an_error():
    assert _verdict(_example(expected=1.0), error="agent timed out") == "error"


def test_a_row_without_expected_is_skipped_not_failed():
    assert _verdict(_example(), calls=[_q(42.0)]) == "skipped"


def test_no_scalar_produced_is_an_error():
    assert _verdict(_example(expected=42.0)) == "error"


def test_matching_within_tolerance():
    assert _verdict(_example(expected=42.0), calls=[_q(42.0)]) == "match"


def test_a_mismatch_records_the_diffs_it_missed_by():
    attempt = Attempt(example=_example(expected=40.0), calls=[_q(42.0)])
    source, actual, _, anchor = _select_answer(attempt)
    status, _, abs_diff, rel_diff = _answer_verdict(attempt, source, actual, anchor)
    assert status == "mismatch"
    assert abs_diff == pytest.approx(2.0)
    assert rel_diff == pytest.approx(0.05)


def test_mismatch_is_anchored_on_the_certified_answer():
    """_compare(actual, expected, ...) -- reversing the operands turns this
    mismatch into a match, because the tolerance would anchor on 100."""
    assert _verdict(_example(expected=1.0, rel_tol=1.0), calls=[_q(100.0)]) == "mismatch"


def test_a_relative_window_makes_the_certified_number_unassertable():
    calls = [_q(42.0, relative_time="CURRENT_DATE")]
    assert _verdict(_example(expected=42.0), calls=calls) == "unassertable"


def test_time_scoped_rows_suppress_the_relative_window_refusal():
    calls = [_q(42.0, relative_time="CURRENT_DATE")]
    assert _verdict(_example(expected=42.0, time_scoped=True), calls=calls) == "match"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_validation/test_conformance.py -k answer_verdict -v`
Expected: FAIL with `ImportError: cannot import name '_answer_verdict'`

- [ ] **Step 3: Write minimal implementation**

```python
from agentic_data_contracts.validation.examples import _compare


def _answer_verdict(
    attempt: Attempt,
    source: str,
    actual: float | None,
    anchor: ToolCall | None,
) -> tuple[str, list[str], float | None, float | None]:
    """Score the number, in order; the first condition that applies wins.

    Returns ``(status, reasons, abs_diff, rel_diff)``. The diffs are ``None``
    on every path that did not reach a comparison.
    """
    if attempt.error is not None:
        return "error", [attempt.error], None, None
    if attempt.example.expected is None:
        return "skipped", [], None, None
    if actual is None:
        return "error", ["no scalar result produced"], None, None
    if (
        anchor is not None
        and anchor.relative_time is not None
        and not attempt.example.time_scoped
    ):
        return "unassertable", [
            f"agent's answering query uses a relative time window "
            f"({anchor.relative_time}); the certified answer decays against it"
        ], None, None

    rel_tol, abs_tol = _tolerances(attempt.example)
    # Argument order is load-bearing: _compare anchors both rel_diff and the
    # tolerance on `expected`, so "within 0.1% of the certified number" stays
    # stable however far the query drifted. Reversed, it would anchor on
    # whatever the agent measured.
    abs_diff, rel_diff, matched = _compare(
        actual, attempt.example.expected, rel_tol, abs_tol
    )
    return ("match" if matched else "mismatch"), [], abs_diff, rel_diff
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_validation/test_conformance.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/validation/conformance.py tests/test_validation/test_conformance.py
git commit -m "feat: the conformance answer verdict"
```

---

### Task 13: The protocol verdict

**Files:**
- Modify: `src/agentic_data_contracts/validation/conformance.py`
- Test: `tests/test_validation/test_conformance.py`

**Interfaces:**
- Produces: `_protocol_verdict(attempt, anchor) -> tuple[str, list[str]]` returning `(status, reasons)`.

Governing principle: **a protocol failure requires a rule the corpus row activated.** No rule activated is `not_applicable` (passes), never a guessed violation. In sub-project 2 these findings drive prose edits, so a false positive writes wrong documentation into the contract.

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_validation/test_conformance.py
from agentic_data_contracts.validation.conformance import _protocol_verdict


def _lookup(name, seq, outcome="ok"):
    return ToolCall(sequence=seq, tool="lookup_metric", args={"metric_name": name}, outcome=outcome)


def _protocol(example, calls=(), **kw):
    attempt = Attempt(example=example, calls=list(calls), **kw)
    _, _, _, anchor = _select_answer(attempt)
    return _protocol_verdict(attempt, anchor)[0]


def test_a_row_activating_no_rule_is_not_applicable():
    assert _protocol(_example(), calls=[_q(42.0)]) == "not_applicable"


def test_a_crashed_attempt_could_not_be_judged():
    assert _protocol(_example(), error="boom") == "unchecked"


def test_declaring_an_answer_with_no_query_is_contaminated():
    assert _protocol(_example(), final_answer=42.0) == "contaminated"


def test_a_foreign_tool_call_is_contaminated():
    assert _protocol(
        _example(), calls=[_q(42.0)], foreign_tool_calls=["mcp__bigquery__execute_sql"]
    ) == "contaminated"


def test_consulting_the_declared_metric_first_is_followed():
    calls = [_lookup("CAC", 0), _q(42.0, seq=1)]
    assert _protocol(_example(expects_metrics=["CAC"]), calls=calls) == "followed"


def test_querying_without_the_declared_lookup_is_violated():
    assert _protocol(_example(expects_metrics=["CAC"]), calls=[_q(42.0)]) == "violated"


def test_a_lookup_after_the_answer_does_not_count():
    calls = [_q(42.0, seq=0), _lookup("CAC", 1)]
    assert _protocol(_example(expects_metrics=["CAC"]), calls=calls) == "violated"


def test_a_fuzzy_miss_does_not_satisfy_the_rule():
    calls = [_lookup("CAC", 0, outcome="miss"), _q(42.0, seq=1)]
    assert _protocol(_example(expects_metrics=["CAC"]), calls=calls) == "violated"


def test_friction_is_recorded_without_failing():
    calls = [
        _lookup("CAC", 0, outcome="miss"),
        _lookup("CAC", 1),
        _q(None, outcome="blocked", seq=2),
        _q(42.0, seq=3),
    ]
    attempt = Attempt(example=_example(expects_metrics=["CAC"]), calls=calls)
    _, _, _, anchor = _select_answer(attempt)
    status, reasons = _protocol_verdict(attempt, anchor)
    assert status == "followed"
    assert any("blocked" in r for r in reasons)
    assert any("miss" in r for r in reasons)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_validation/test_conformance.py -k protocol -v`
Expected: FAIL with `ImportError: cannot import name '_protocol_verdict'`

- [ ] **Step 3: Write minimal implementation**

```python
def _protocol_verdict(attempt: Attempt, anchor: ToolCall | None) -> tuple[str, list[str]]:
    """Judge whether the agent used the governed path.

    Only rules the corpus row *activated* can fail it. A row that declares
    nothing lands on ``not_applicable``, which passes -- a guessed violation
    here would become wrongly-rewritten contract prose downstream.
    """
    reasons: list[str] = []

    # P3 -- friction. Recorded on every path, never fails on its own.
    blocked = [c for c in attempt.calls if c.outcome == "blocked"]
    if blocked:
        reasons.append(
            f"{len(blocked)} blocked query attempt(s) before an accepted one"
        )
    misses = [c for c in attempt.calls if c.outcome == "miss"]
    for call in misses:
        reasons.append(f"lookup miss on {call.tool}({call.args})")

    if attempt.error is not None:
        return "unchecked", reasons

    successful = [c for c in attempt.calls if c.tool == "run_query" and c.outcome == "ok"]

    # P1 -- contamination.
    if attempt.foreign_tool_calls:
        reasons.append(
            f"non-contract tools were available: {', '.join(attempt.foreign_tool_calls)}"
        )
        return "contaminated", reasons
    if attempt.final_answer is not None and not successful:
        reasons.append(
            "an answer was declared with no successful run_query — it came from "
            "outside the governed path"
        )
        return "contaminated", reasons

    # P2 -- metric consultation. Activated only by expects_metrics.
    if not attempt.example.expects_metrics:
        return "not_applicable", reasons
    if anchor is None:
        reasons.append("no answering query to order the metric lookups against")
        return "unchecked", reasons

    consulted = {
        str(c.args.get("metric_name", ""))
        for c in attempt.calls
        if c.tool == "lookup_metric" and c.outcome == "ok" and c.sequence < anchor.sequence
    }
    missing = [m for m in attempt.example.expects_metrics if m not in consulted]
    if missing:
        reasons.append(
            f"answered without consulting {', '.join(missing)} — "
            "lookup_metric was never called for it before the answering query"
        )
        return "violated", reasons
    return "followed", reasons
```

Note the second contamination clause covers only the *declared* case. A derived scalar cannot coexist with zero successful `run_query` calls, because candidates are drawn exclusively from successful `run_query` calls — that branch would be dead by construction.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_validation/test_conformance.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/validation/conformance.py tests/test_validation/test_conformance.py
git commit -m "feat: the conformance protocol verdict"
```

---

### Task 14: The result, the report, and evaluate_conformance

**Files:**
- Modify: `src/agentic_data_contracts/validation/conformance.py`
- Test: `tests/test_validation/test_conformance.py`

**Interfaces:**
- Consumes: Tasks 11–13; `_label` from `validation/examples.py`.
- Produces: `ConformanceResult`, `ConformanceReport`, `evaluate_conformance(attempts: list[Attempt]) -> ConformanceReport`.

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_validation/test_conformance.py
from agentic_data_contracts.validation.conformance import (
    ConformanceReport,
    evaluate_conformance,
)


def _attempt(example, calls=(), **kw):
    return Attempt(example=example, calls=list(calls), **kw)


def test_a_clean_run_passes_the_gate():
    report = evaluate_conformance([_attempt(_example(expected=42.0), [_q(42.0)])])
    assert report.ok is True
    assert report.pass_rate() == 1.0


def test_an_empty_report_is_not_ok():
    """An emptied or fully-filtered corpus must surface, not pass a no-op gate."""
    assert evaluate_conformance([]).ok is False


def test_a_protocol_only_row_passes_without_an_expected():
    assert evaluate_conformance([_attempt(_example(), [_q(42.0)])]).ok is True


def test_an_ambiguous_answer_selection_fails_the_gate():
    attempts = [_attempt(_example(expected=42.0), [_q(42.0, seq=0), _q(4182.0, seq=1)])]
    report = evaluate_conformance(attempts)
    assert report.ok is False
    assert len(report.ambiguous) == 1


def test_ambiguity_on_a_protocol_only_row_does_not_fail_the_gate():
    """Nobody asserted an answer, so its ambiguity is irrelevant."""
    attempts = [_attempt(_example(), [_q(42.0, seq=0), _q(4182.0, seq=1)])]
    assert evaluate_conformance(attempts).ok is True


def test_declaring_the_answer_resolves_ambiguity():
    attempts = [
        _attempt(
            _example(expected=42.0),
            [_q(42.0, seq=0), _q(4182.0, seq=1)],
            final_answer=42.0,
        )
    ]
    assert evaluate_conformance(attempts).ok is True


def test_repeats_group_by_example_id():
    ex = _example(id="q1", expected=42.0)
    report = evaluate_conformance([_attempt(ex, [_q(42.0)]) for _ in range(3)])
    assert list(report.by_example()) == ["q1"]
    assert len(report.by_example()["q1"]) == 3


def test_one_flake_in_three_repeats_fails_the_strict_gate():
    ex = _example(id="q1", expected=42.0)
    attempts = [_attempt(ex, [_q(42.0)]), _attempt(ex, [_q(42.0)]), _attempt(ex, [_q(0.0)])]
    report = evaluate_conformance(attempts)
    assert report.ok is False
    assert report.pass_rate() == pytest.approx(2 / 3)


def test_summary_is_markdown_and_never_prints_raw_sql():
    sql = "SELECT secret_column FROM analytics.orders"
    attempts = [_attempt(_example(expected=42.0), [_q(42.0, args={"sql": sql})])]
    text = evaluate_conformance(attempts).summary()
    assert "|" in text
    assert sql not in text
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_validation/test_conformance.py -k evaluate -v`
Expected: FAIL with `ImportError: cannot import name 'evaluate_conformance'`

- [ ] **Step 3: Write minimal implementation**

```python
from agentic_data_contracts.validation.examples import _label


@dataclass
class ConformanceResult:
    """The verdict for one attempt, on two orthogonal axes.

    ``answer_source`` stays separate from ``answer`` rather than fusing into it:
    a ``last_scalar`` row that numerically matched still reports
    ``answer="match"`` and is still excluded from ``ok``. The verdict and the
    evidence for it are different fields, so nothing hides how it was derived.
    """

    attempt: Attempt
    answer: str
    protocol: str
    answer_source: str
    scalar_candidates: int
    expected: float | None = None
    actual: float | None = None
    abs_diff: float | None = None
    rel_diff: float | None = None
    rel_tol: float = _DEFAULT_REL_TOL
    abs_tol: float = _DEFAULT_ABS_TOL
    reasons: list[str] = field(default_factory=list)
    label: str = ""


@dataclass
class ConformanceReport:
    results: list[ConformanceResult]

    @property
    def passed(self) -> list[ConformanceResult]:
        return [r for r in self.results if _result_ok(r)]

    @property
    def answer_failures(self) -> list[ConformanceResult]:
        return [r for r in self.results if r.answer in {"mismatch", "error", "unassertable"}]

    @property
    def protocol_failures(self) -> list[ConformanceResult]:
        return [r for r in self.results if r.protocol == "violated"]

    @property
    def contaminated(self) -> list[ConformanceResult]:
        return [r for r in self.results if r.protocol == "contaminated"]

    @property
    def ambiguous(self) -> list[ConformanceResult]:
        return [
            r
            for r in self.results
            if r.answer == "match" and r.answer_source == "last_scalar"
        ]

    @property
    def skipped(self) -> list[ConformanceResult]:
        return [r for r in self.results if r.answer == "skipped"]

    def by_example(self) -> dict[str, list[ConformanceResult]]:
        """Group repeats of the same question.

        Keys on ``id``, falling back to ``question`` -- never on ``label``,
        which embeds a positional index and would split repeats apart.
        """
        grouped: dict[str, list[ConformanceResult]] = {}
        for result in self.results:
            example = result.attempt.example
            key = example.id or example.question or result.label
            grouped.setdefault(key, []).append(result)
        return grouped

    def pass_rate(self) -> float:
        if not self.results:
            return 0.0
        return len(self.passed) / len(self.results)

    @property
    def ok(self) -> bool:
        """The strict safe gate. An empty report is not ok."""
        return bool(self.results) and all(_result_ok(r) for r in self.results)

    def summary(self) -> str:
        lines = [
            f"**Conformance:** {len(self.passed)}/{len(self.results)} attempts passed "
            f"({self.pass_rate():.0%})",
            "",
            "| Example | Answer | Protocol | Source | Notes |",
            "| --- | --- | --- | --- | --- |",
        ]
        for r in self.results:
            notes = "; ".join(r.reasons) if r.reasons else ""
            lines.append(
                f"| {r.label} | {r.answer} | {r.protocol} | {r.answer_source} | {notes} |"
            )
        return "\n".join(lines)


def _result_ok(result: ConformanceResult) -> bool:
    """Nothing-to-judge passes; couldn't-judge fails.

    The ``last_scalar`` exclusion is scoped to rows whose answer axis was
    actually judged: ``answer_source`` is derived for every attempt, so a
    protocol-only row where the agent ran several exploratory queries would
    otherwise fail the gate over an answer nobody was asserting.
    """
    if result.protocol not in {"followed", "not_applicable"}:
        return False
    if result.answer == "skipped":
        return True
    return result.answer == "match" and result.answer_source != "last_scalar"


def evaluate_conformance(attempts: list[Attempt]) -> ConformanceReport:
    """Score recorded attempts. Pure: no network, no database, no model."""
    results = []
    for index, attempt in enumerate(attempts):
        source, actual, candidates, anchor = _select_answer(attempt)
        answer, answer_reasons, abs_diff, rel_diff = _answer_verdict(
            attempt, source, actual, anchor
        )
        protocol, protocol_reasons = _protocol_verdict(attempt, anchor)
        rel_tol, abs_tol = _tolerances(attempt.example)
        results.append(
            ConformanceResult(
                attempt=attempt,
                answer=answer,
                protocol=protocol,
                answer_source=source,
                scalar_candidates=candidates,
                expected=attempt.example.expected,
                actual=actual,
                abs_diff=abs_diff,
                rel_diff=rel_diff,
                rel_tol=rel_tol,
                abs_tol=abs_tol,
                reasons=answer_reasons + protocol_reasons,
                label=_label(attempt.example, index),
            )
        )
    return ConformanceReport(results=results)
```

`summary()` prints `reasons` and `label` but never `ToolCall.args`, so SQL literals stay out of PR comments while remaining available on the record.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_validation/test_conformance.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/validation/conformance.py tests/test_validation/test_conformance.py
git commit -m "feat: ConformanceReport and evaluate_conformance"
```

---

### Task 15: Public exports

**Files:**
- Modify: `src/agentic_data_contracts/validation/__init__.py`
- Modify: `tests/test_public_api.py`

**Interfaces:**
- Produces: `Attempt`, `ConformanceReport`, `ConformanceResult`, `ToolCall`, `ToolRecorder`, `evaluate_conformance` importable from `agentic_data_contracts.validation`.

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_public_api.py
def test_conformance_names_are_exported():
    from agentic_data_contracts import validation

    for name in [
        "Attempt",
        "ConformanceReport",
        "ConformanceResult",
        "ToolCall",
        "ToolRecorder",
        "evaluate_conformance",
    ]:
        assert hasattr(validation, name), name
        assert name in validation.__all__, name
```

Match the style of the existing assertions in that file rather than copying this shape verbatim if it already has a helper for the same check.

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_public_api.py -k conformance -v`
Expected: FAIL with `AssertionError: Attempt`

- [ ] **Step 3: Write minimal implementation**

In `validation/__init__.py` add, in the existing alphabetical import order:

```python
from agentic_data_contracts.core.recorder import ToolCall, ToolRecorder
from agentic_data_contracts.validation.conformance import (
    Attempt,
    ConformanceReport,
    ConformanceResult,
    evaluate_conformance,
)
```

Re-exporting the two recorder types from `validation` gives consumers a single import site even though they live in `core/` for cycle reasons. Add all six names to `__all__`, keeping its existing sort order.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/ -v`
Expected: PASS — the whole suite

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/validation/__init__.py tests/test_public_api.py
git commit -m "feat: export the conformance evaluator"
```

---

### Task 16: End-to-end with scripted agents

**Files:**
- Create: `tests/test_validation/test_conformance_e2e.py`

**Interfaces:**
- Consumes: everything above. Adds no production code.

Tiers 1 and 2 test the verdicts and the recorder separately. This proves they compose: real tools, real recorder, real session, real verdicts — driven by a plain async function instead of a model, so it is deterministic and free.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_validation/test_conformance_e2e.py
import pytest

from agentic_data_contracts.core.recorder import ToolRecorder
from agentic_data_contracts.core.session import ContractSession
from agentic_data_contracts.tools.factory import create_tools
from agentic_data_contracts.validation import Attempt, evaluate_conformance
from agentic_data_contracts.validation.examples import VerifiedExample


def _run(contract, adapter, recorder):
    session = ContractSession(contract, recorder=recorder)
    tools = {t.name: t.callable for t in create_tools(contract, adapter=adapter, session=session)}
    return session, tools


@pytest.mark.asyncio
async def test_a_compliant_scripted_agent_passes(contract, adapter):
    example = VerifiedExample(
        sql="SELECT COUNT(id) FROM analytics.orders WHERE tenant_id = 'acme'",
        question="How many orders does acme have?",
        id="orders-count",
        expected=2.0,
        expects_metrics=["total_revenue"],
    )
    rec = ToolRecorder()
    session, tools = _run(contract, adapter, rec)

    await tools["lookup_metric"]({"metric_name": "total_revenue"})
    await tools["run_query"]({"sql": example.sql})

    report = evaluate_conformance([Attempt.from_session(example, session)])
    assert report.ok is True


@pytest.mark.asyncio
async def test_skipping_the_declared_lookup_is_violated(contract, adapter):
    example = VerifiedExample(
        sql="SELECT COUNT(id) FROM analytics.orders WHERE tenant_id = 'acme'",
        question="How many orders does acme have?",
        expected=2.0,
        expects_metrics=["total_revenue"],
    )
    rec = ToolRecorder()
    session, tools = _run(contract, adapter, rec)

    await tools["run_query"]({"sql": example.sql})

    report = evaluate_conformance([Attempt.from_session(example, session)])
    assert report.ok is False
    assert len(report.protocol_failures) == 1


@pytest.mark.asyncio
async def test_an_out_of_band_answer_is_contaminated(contract, adapter):
    example = VerifiedExample(sql="SELECT 1", question="q", expected=2.0)
    rec = ToolRecorder()
    session, _ = _run(contract, adapter, rec)

    report = evaluate_conformance(
        [Attempt.from_session(example, session, final_answer=3.0)]
    )
    assert len(report.contaminated) == 1
    assert report.ok is False


@pytest.mark.asyncio
async def test_a_rerun_after_a_blocked_attempt_still_passes(contract, adapter):
    """Friction is recorded; it must not fail the gate on its own."""
    example = VerifiedExample(
        sql="SELECT COUNT(id) FROM analytics.orders WHERE tenant_id = 'acme'",
        question="q",
        expected=2.0,
    )
    rec = ToolRecorder()
    session, tools = _run(contract, adapter, rec)

    await tools["run_query"]({"sql": "SELECT * FROM analytics.orders"})  # blocked
    await tools["run_query"]({"sql": example.sql})
    await tools["run_query"]({"sql": example.sql})  # identical rerun

    report = evaluate_conformance([Attempt.from_session(example, session)])
    assert report.ok is True
    assert report.results[0].answer_source == "sole_scalar"
    assert any("blocked" in r for r in report.results[0].reasons)
```

Adjust `expected=2.0` and every SQL string to the actual fixture data and contract rules. Run the query by hand against the fixture first if the row count is not obvious.

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_validation/test_conformance_e2e.py -v`
Expected: FAIL on fixture or expected-value mismatches — fix those, not the production code

- [ ] **Step 3: Make the tests pass**

No production code changes. Correct the fixture names, SQL, and expected values until all four pass. If a test fails for a *behavioral* reason rather than a fixture reason, stop: that is a real defect in Tasks 1–14 and must be fixed there with its own unit test.

- [ ] **Step 4: Run the whole suite**

Run: `uv run pytest -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/test_validation/test_conformance_e2e.py
git commit -m "test: end-to-end conformance evaluation with scripted agents"
```

---

### Task 17: The runnable demo

**Files:**
- Create: `examples/revenue_agent/evaluate_conformance.py`
- Modify: `examples/revenue_agent/verified_examples.yml`

**Interfaces:**
- Consumes: the public API from Task 15.

The README promises every example runs in demo mode with no API key. This one must too, so the "agent" is a scripted function.

- [ ] **Step 1: Add questions and one expects_metrics to the corpus**

Open `examples/revenue_agent/verified_examples.yml`. Every row that has an `expected` needs a `question`; add one where missing, phrased as a user would ask it. Add `expects_metrics: [total_revenue]` to exactly one row whose certified SQL uses the governed revenue definition.

- [ ] **Step 2: Write the demo script**

```python
# examples/revenue_agent/evaluate_conformance.py
"""Pass 3 over the revenue corpus: can an agent reach the certified answer?

Runs with no API key. The "agent" here is a scripted function -- the point is
to show the harness end to end, not to benchmark a model. Swap
``_scripted_agent`` for a real agent loop to get a real measurement.
"""

import asyncio
import sys
from pathlib import Path

from agentic_data_contracts import DataContract
from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.core.recorder import ToolRecorder
from agentic_data_contracts.core.session import ContractSession
from agentic_data_contracts.tools.factory import create_tools
from agentic_data_contracts.validation import Attempt, evaluate_conformance
from agentic_data_contracts.validation.examples import VerifiedExample

HERE = Path(__file__).parent


async def _scripted_agent(example: VerifiedExample, tools: dict) -> str:
    """Stand-in for a model: consult declared metrics, then run the query."""
    for metric in example.expects_metrics:
        await tools["lookup_metric"]({"metric_name": metric})
    await tools["run_query"]({"sql": example.sql})
    return "answered"


async def main() -> int:
    contract = DataContract.from_yaml(HERE / "contract.yml")
    adapter = DuckDBAdapter(str(HERE / "sample_data.duckdb"))
    corpus = [
        ex
        for ex in load_corpus(HERE / "verified_examples.yml")
        if ex.question
    ]

    attempts = []
    for example in corpus:
        session = ContractSession(contract, recorder=ToolRecorder())
        tools = {
            t.name: t.callable
            for t in create_tools(contract, adapter=adapter, session=session)
        }
        text = await _scripted_agent(example, tools)
        attempts.append(Attempt.from_session(example, session, final_text=text))

    report = evaluate_conformance(attempts)
    print(report.summary())
    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
```

Replace `load_corpus` and `DataContract.from_yaml` with whatever `verify_examples.py` in the same directory already uses to load the contract and the corpus — read it first and mirror it exactly, including the adapter construction.

- [ ] **Step 3: Run the demo**

Run: `uv run python examples/revenue_agent/evaluate_conformance.py`
Expected: a markdown table, and exit code 0

- [ ] **Step 4: Commit**

```bash
git add examples/revenue_agent/evaluate_conformance.py examples/revenue_agent/verified_examples.yml
git commit -m "docs: runnable conformance evaluation demo for revenue_agent"
```

---

### Task 18: README and final verification

**Files:**
- Modify: `README.md`
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Document the third pass**

Add a section immediately after "Validating a verified-examples corpus", titled `### Evaluating agent conformance`. It must cover:

- The three passes as a progression: `validate_examples` asks whether the contract is still *enforceable*; `check_example_answers` whether it is still *accurate*; `evaluate_conformance` whether it is still *teachable*. Only the third degrades silently today.
- The consumer-loop code sample from the spec's Architecture section, verbatim.
- The two axes, their five states each, and the nothing-to-judge / couldn't-judge distinction.
- The closed-world requirement and the `contaminated` status, including the documented limit: an agent that used `run_query` but drew context from a foreign retriever leaves no detectable trace.
- The CI table from the spec's "CI integration guidance" section, including that pass 3 is nondeterministic, costs money, and should be path-filtered or nightly rather than a hard per-PR gate.
- That adopting it starts with populating `question` on corpus rows.

- [ ] **Step 2: Add a CHANGELOG entry**

Follow the existing format. Note the new public names, the `expects_metrics` field, and that `ContractSession` takes a keyword-only `recorder`. Flag the two internal refactors (`_scalar_value`, `_timewindow`) as non-breaking.

- [ ] **Step 3: Run the full verification**

```bash
uv run pytest -v
prek run --all-files
uv run python examples/revenue_agent/evaluate_conformance.py
uv run python examples/revenue_agent/verify_examples.py
```

Expected: all pass. The last command confirms passes 1 and 2 still work after the `_scalar` and `_timewindow` refactors.

- [ ] **Step 4: Confirm no dependency drift**

```bash
git diff main --stat -- pyproject.toml uv.lock
```

Expected: `pyproject.toml` shows only the sqlglot floor *comment* change from Task 4. `uv.lock` unchanged. If either shows more, a dependency was added — revert it.

- [ ] **Step 5: Commit**

```bash
git add README.md CHANGELOG.md
git commit -m "docs: document the contract conformance evaluator"
```
