# Metric Decomposition Reconciliation Check Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a validation-layer function that executes a metric's declared arithmetic `decompositions` against a live database (caller-supplied scalar SQL) and asserts the identity holds within tolerance.

**Architecture:** One new module `validation/reconciliation.py` exposing `reconcile_decomposition(metric, *, parent_sql, operand_sql, adapter, ...) -> ReconciliationResult`. It reads the operator/operands from the metric's declared decomposition (the contract owns *what* the identity is), executes caller-supplied scalar SQL for the parent and each operand via the existing `DatabaseAdapter.execute`, applies the operator, and compares within tolerance. It reports the discrepancy and numbers; it never infers the cause. Built bottom-up: result type + scalar execution → operator application → orchestration.

**Tech Stack:** Python 3.12+, existing `DatabaseAdapter` protocol and `MetricDefinition`/`Decomposition` models, DuckDB for tests. No new dependencies.

## Global Constraints

- **Python ≥ 3.12**, run everything with `uv run`.
- **No new dependencies** — use only `DatabaseAdapter`, `QueryResult`, `MetricDefinition`, `Decomposition` and the stdlib (`math`, `dataclasses`, `collections.abc`).
- **Lint/type-check through prek**, never bare `ruff`/`ty`: `prek run --all-files`.
- **TDD**: write the failing test, watch it fail, implement minimally, watch it pass, commit.
- **Synchronous** function (`adapter.execute` is sync; CI/test is the primary home).
- **Default `rel_tol=1e-4`** (decompositions are exact identities; tolerance covers only float/`FILTER`/`DISTINCT`/division noise). `abs_tol` default `0.0`.
- **Governance/agent boundary**: the result reports numbers and a mechanical `reason` (e.g. "returned NULL", "denominator is zero", "does not hold within tolerance"). It must NOT infer interpretive causes ("population differs", "tautological").
- **Export** `reconcile_decomposition` and `ReconciliationResult` from `validation/__init__.py` only (matching how `Validator` and the checkers are exposed — not re-exported at top level).
- **Frozen result**: `ReconciliationResult` is a `@dataclass(frozen=True)`.

---

### Task 1: Result type + scalar execution helper

**Files:**
- Create: `src/agentic_data_contracts/validation/reconciliation.py`
- Create: `tests/test_validation/test_reconciliation.py`

**Interfaces:**
- Consumes: `DatabaseAdapter` from `agentic_data_contracts.adapters.base` (`.execute(sql) -> QueryResult`, `QueryResult.columns: list[str]`, `QueryResult.rows: list[tuple]`).
- Produces:
  - `ReconciliationResult` — frozen dataclass with fields `metric: str`, `operator: str`, `operands: dict[str, float]`, `implied_parent: float`, `actual_parent: float`, `abs_diff: float`, `rel_diff: float`, `reconciles: bool`, `rel_tol: float`, `abs_tol: float`, `reason: str | None = None`.
  - `_scalar(adapter: DatabaseAdapter, sql: str, label: str) -> float | None` — executes `sql`, returns its single scalar as `float`, `None` if the result is empty or the value is SQL `NULL`; raises `ValueError` if the query does not return exactly one column and at most one row.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_validation/test_reconciliation.py`:

```python
import pytest

from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.validation.reconciliation import (
    ReconciliationResult,
    _scalar,
)


@pytest.fixture
def adapter() -> DuckDBAdapter:
    return DuckDBAdapter(":memory:")


class TestScalar:
    def test_returns_float(self, adapter: DuckDBAdapter) -> None:
        assert _scalar(adapter, "SELECT 5", "x") == 5.0

    def test_null_value_returns_none(self, adapter: DuckDBAdapter) -> None:
        assert _scalar(adapter, "SELECT NULL", "x") is None

    def test_empty_result_returns_none(self, adapter: DuckDBAdapter) -> None:
        assert _scalar(adapter, "SELECT 1 WHERE false", "x") is None

    def test_multi_column_raises(self, adapter: DuckDBAdapter) -> None:
        with pytest.raises(ValueError, match="exactly one column"):
            _scalar(adapter, "SELECT 1 AS a, 2 AS b", "x")

    def test_multi_row_raises(self, adapter: DuckDBAdapter) -> None:
        with pytest.raises(ValueError, match="at most one row"):
            _scalar(adapter, "SELECT * FROM (VALUES (1), (2)) AS t(x)", "x")


class TestResultType:
    def test_is_frozen(self) -> None:
        r = ReconciliationResult(
            metric="m",
            operator="sum",
            operands={"a": 1.0},
            implied_parent=1.0,
            actual_parent=1.0,
            abs_diff=0.0,
            rel_diff=0.0,
            reconciles=True,
            rel_tol=1e-4,
            abs_tol=0.0,
        )
        assert r.reason is None
        with pytest.raises(AttributeError):
            r.reconciles = False  # type: ignore[misc]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_validation/test_reconciliation.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'agentic_data_contracts.validation.reconciliation'`

- [ ] **Step 3: Write minimal implementation**

Create `src/agentic_data_contracts/validation/reconciliation.py`:

```python
"""Reconcile a metric's declared arithmetic decomposition against live data.

Executes the parent metric and each declared operand (via caller-supplied
scalar SQL), applies the decomposition operator, and checks the identity holds
within tolerance. This is a contract-integrity check: it catches an identity
that has become false in the data (ETL drift, definition drift) that the
per-query validators never see. It reports the discrepancy and the numbers; it
does not infer the cause (that is agent-owned diagnosis).
"""

from __future__ import annotations

from dataclasses import dataclass

from agentic_data_contracts.adapters.base import DatabaseAdapter


@dataclass(frozen=True)
class ReconciliationResult:
    metric: str
    operator: str
    operands: dict[str, float]
    implied_parent: float
    actual_parent: float
    abs_diff: float
    rel_diff: float
    reconciles: bool
    rel_tol: float
    abs_tol: float
    reason: str | None = None


def _scalar(adapter: DatabaseAdapter, sql: str, label: str) -> float | None:
    """Return the single scalar value of ``sql``, or ``None`` if empty/NULL.

    Raises ``ValueError`` if the query does not return exactly one column and at
    most one row — a reconciliation operand must be a scalar.
    """
    result = adapter.execute(sql)
    if len(result.columns) != 1:
        raise ValueError(
            f"{label} query must return exactly one column, got {len(result.columns)}"
        )
    if len(result.rows) > 1:
        raise ValueError(
            f"{label} query must return at most one row, got {len(result.rows)}"
        )
    if not result.rows:
        return None
    value = result.rows[0][0]
    if value is None:
        return None
    return float(value)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_validation/test_reconciliation.py -v`
Expected: PASS (6 passed)

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/validation/reconciliation.py tests/test_validation/test_reconciliation.py
git commit -m "feat: reconciliation result type and scalar execution helper"
```

---

### Task 2: Operator application

**Files:**
- Modify: `src/agentic_data_contracts/validation/reconciliation.py`
- Modify: `tests/test_validation/test_reconciliation.py`

**Interfaces:**
- Produces: `_apply_operator(operator: str, values: list[float]) -> float` — folds `values` (in declared order) by operator: `sum` → `math.fsum(values)`; `product` → running product; `ratio` → `values[0] / values[1]`; `difference` → `values[0] - values[1]`. Raises `ValueError` on an unknown operator. Assumes arity already validated by `validate_decompositions()` at load time (`ratio`/`difference` binary; `sum`/`product` ≥ 2).

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_validation/test_reconciliation.py` (add `_apply_operator` to the existing import from `...reconciliation`):

```python
class TestApplyOperator:
    def test_sum(self) -> None:
        from agentic_data_contracts.validation.reconciliation import _apply_operator

        assert _apply_operator("sum", [1.0, 2.0, 3.0]) == 6.0

    def test_product(self) -> None:
        from agentic_data_contracts.validation.reconciliation import _apply_operator

        assert _apply_operator("product", [2.0, 3.0, 4.0]) == 24.0

    def test_ratio(self) -> None:
        from agentic_data_contracts.validation.reconciliation import _apply_operator

        assert _apply_operator("ratio", [3.0, 4.0]) == 0.75

    def test_difference(self) -> None:
        from agentic_data_contracts.validation.reconciliation import _apply_operator

        assert _apply_operator("difference", [10.0, 4.0]) == 6.0

    def test_unknown_operator_raises(self) -> None:
        from agentic_data_contracts.validation.reconciliation import _apply_operator

        with pytest.raises(ValueError, match="unknown decomposition operator"):
            _apply_operator("power", [2.0, 3.0])
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_validation/test_reconciliation.py::TestApplyOperator -v`
Expected: FAIL with `ImportError: cannot import name '_apply_operator'`

- [ ] **Step 3: Write minimal implementation**

In `src/agentic_data_contracts/validation/reconciliation.py`, add `import math` at the top of the imports block:

```python
import math
from dataclasses import dataclass
```

Then add, after `_scalar`:

```python
def _apply_operator(operator: str, values: list[float]) -> float:
    """Fold ``values`` (in declared order) by the decomposition operator."""
    if operator == "sum":
        return math.fsum(values)
    if operator == "product":
        product = 1.0
        for value in values:
            product *= value
        return product
    if operator == "ratio":
        return values[0] / values[1]
    if operator == "difference":
        return values[0] - values[1]
    raise ValueError(f"unknown decomposition operator: {operator!r}")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_validation/test_reconciliation.py::TestApplyOperator -v`
Expected: PASS (5 passed)

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/validation/reconciliation.py tests/test_validation/test_reconciliation.py
git commit -m "feat: decomposition operator application"
```

---

### Task 3: reconcile_decomposition orchestration + export

**Files:**
- Modify: `src/agentic_data_contracts/validation/reconciliation.py`
- Modify: `src/agentic_data_contracts/validation/__init__.py`
- Modify: `tests/test_validation/test_reconciliation.py`

**Interfaces:**
- Consumes: `_scalar` (Task 1), `_apply_operator` (Task 2), `MetricDefinition`/`Decomposition` from `agentic_data_contracts.semantic.base`.
- Produces: `reconcile_decomposition(metric, *, parent_sql, operand_sql, adapter, rel_tol=1e-4, abs_tol=0.0, decomposition=0) -> ReconciliationResult`. Reads `metric.decompositions[decomposition]` for operator + operand names; requires `operand_sql` keys to exactly equal the declared operands; measures each operand (declared order) and the parent; a NULL/empty measurement → `reconciles=False` with reason; `ratio` denominator `0` → `reconciles=False` with reason; otherwise computes `implied_parent`, `abs_diff`, `rel_diff`, and `reconciles = abs_diff <= max(abs_tol, rel_tol * abs(actual_parent))`. Raises `ValueError` on no declared decomposition, out-of-range index, or operand-key mismatch.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_validation/test_reconciliation.py`. Add these imports at the top of the file (alongside the existing imports):

```python
from agentic_data_contracts.semantic.base import Decomposition, MetricDefinition
from agentic_data_contracts.validation.reconciliation import reconcile_decomposition


def _metric(operator: str, operands: list[str], name: str = "parent") -> MetricDefinition:
    return MetricDefinition(
        name=name,
        description="",
        sql_expression="<parent expr>",
        decompositions=[Decomposition(operator=operator, operands=operands)],
    )
```

Then the test classes:

```python
class TestReconcileHappyPath:
    def test_product_holds(self, adapter: DuckDBAdapter) -> None:
        metric = _metric("product", ["a", "b"])
        result = reconcile_decomposition(
            metric,
            parent_sql="SELECT 20",
            operand_sql={"a": "SELECT 4", "b": "SELECT 5"},
            adapter=adapter,
        )
        assert result.reconciles is True
        assert result.implied_parent == 20.0
        assert result.actual_parent == 20.0
        assert result.operands == {"a": 4.0, "b": 5.0}
        assert result.reason is None

    def test_ratio_holds(self, adapter: DuckDBAdapter) -> None:
        metric = _metric("ratio", ["num", "den"])
        result = reconcile_decomposition(
            metric,
            parent_sql="SELECT 0.75",
            operand_sql={"num": "SELECT 3", "den": "SELECT 4"},
            adapter=adapter,
        )
        assert result.reconciles is True

    def test_breaks_beyond_tolerance(self, adapter: DuckDBAdapter) -> None:
        metric = _metric("product", ["a", "b"])
        result = reconcile_decomposition(
            metric,
            parent_sql="SELECT 21",
            operand_sql={"a": "SELECT 4", "b": "SELECT 5"},
            adapter=adapter,
        )
        assert result.reconciles is False
        assert result.implied_parent == 20.0
        assert result.actual_parent == 21.0
        assert result.reason == "identity does not hold within tolerance"

    def test_tolerance_boundary_inclusive(self, adapter: DuckDBAdapter) -> None:
        # implied 20, actual 16, abs_diff 4.0 == rel_tol(0.25)*16 -> reconciles.
        # All values are exact binary floats, so the boundary is not subject to
        # rounding (avoid decimals like 0.002 that are inexact in float).
        metric = _metric("product", ["a", "b"])
        result = reconcile_decomposition(
            metric,
            parent_sql="SELECT 16",
            operand_sql={"a": "SELECT 4", "b": "SELECT 5"},
            adapter=adapter,
            rel_tol=0.25,
        )
        assert result.reconciles is True

    def test_tolerance_boundary_exclusive(self, adapter: DuckDBAdapter) -> None:
        # implied 20, actual 16, abs_diff 4.0 > rel_tol(0.125)*16 = 2.0 -> no
        metric = _metric("product", ["a", "b"])
        result = reconcile_decomposition(
            metric,
            parent_sql="SELECT 16",
            operand_sql={"a": "SELECT 4", "b": "SELECT 5"},
            adapter=adapter,
            rel_tol=0.125,
        )
        assert result.reconciles is False


class TestReconcileFindings:
    def test_null_operand_is_finding(self, adapter: DuckDBAdapter) -> None:
        metric = _metric("product", ["a", "b"])
        result = reconcile_decomposition(
            metric,
            parent_sql="SELECT 20",
            operand_sql={"a": "SELECT NULL", "b": "SELECT 5"},
            adapter=adapter,
        )
        assert result.reconciles is False
        assert result.reason is not None and "NULL" in result.reason
        assert "a" in result.reason

    def test_ratio_zero_denominator_is_finding(self, adapter: DuckDBAdapter) -> None:
        metric = _metric("ratio", ["num", "den"])
        result = reconcile_decomposition(
            metric,
            parent_sql="SELECT 0",
            operand_sql={"num": "SELECT 3", "den": "SELECT 0"},
            adapter=adapter,
        )
        assert result.reconciles is False
        assert result.reason is not None and "denominator" in result.reason


class TestReconcilePreconditions:
    def test_operand_key_mismatch_raises(self, adapter: DuckDBAdapter) -> None:
        metric = _metric("product", ["a", "b"])
        with pytest.raises(ValueError, match="do not match the declared operands"):
            reconcile_decomposition(
                metric,
                parent_sql="SELECT 20",
                operand_sql={"a": "SELECT 4", "WRONG": "SELECT 5"},
                adapter=adapter,
            )

    def test_no_decomposition_raises(self, adapter: DuckDBAdapter) -> None:
        metric = MetricDefinition(name="leaf", description="", sql_expression="x")
        with pytest.raises(ValueError, match="no decompositions"):
            reconcile_decomposition(
                metric, parent_sql="SELECT 1", operand_sql={}, adapter=adapter
            )

    def test_index_out_of_range_raises(self, adapter: DuckDBAdapter) -> None:
        metric = _metric("product", ["a", "b"])
        with pytest.raises(ValueError, match="out of range"):
            reconcile_decomposition(
                metric,
                parent_sql="SELECT 20",
                operand_sql={"a": "SELECT 4", "b": "SELECT 5"},
                adapter=adapter,
                decomposition=1,
            )

    def test_decomposition_index_selects_second(self, adapter: DuckDBAdapter) -> None:
        metric = MetricDefinition(
            name="parent",
            description="",
            sql_expression="x",
            decompositions=[
                Decomposition(operator="product", operands=["a", "b"]),
                Decomposition(operator="sum", operands=["c", "d"]),
            ],
        )
        result = reconcile_decomposition(
            metric,
            parent_sql="SELECT 9",
            operand_sql={"c": "SELECT 4", "d": "SELECT 5"},
            adapter=adapter,
            decomposition=1,
        )
        assert result.operator == "sum"
        assert result.reconciles is True


class TestReconcilePopulationMismatch:
    def test_conversion_rate_population_mismatch(self, adapter: DuckDBAdapter) -> None:
        # Parent joins events->users, so its denominator counts only users who
        # appear in events (1,2,3). cohort_signups counts ALL cohort users
        # (1,2,3,4). The identity conversion_rate = first_purchase_users /
        # cohort_signups therefore does NOT hold: 2/3 (parent) vs 2/4 (implied).
        adapter.connection.execute(
            """
            CREATE SCHEMA analytics;
            CREATE TABLE analytics.users (id INTEGER, cohort_month VARCHAR);
            INSERT INTO analytics.users VALUES
                (1, '2026-05'), (2, '2026-05'), (3, '2026-05'), (4, '2026-05');
            CREATE TABLE analytics.events (user_id INTEGER, event_name VARCHAR);
            INSERT INTO analytics.events VALUES
                (1, 'first_purchase'), (2, 'first_purchase'), (3, 'login');
            """
        )
        metric = _metric(
            "ratio", ["first_purchase_users", "cohort_signups"], name="conversion_rate"
        )
        result = reconcile_decomposition(
            metric,
            parent_sql=(
                "SELECT COUNT(DISTINCT e.user_id) "
                "FILTER (WHERE e.event_name = 'first_purchase') "
                "/ COUNT(DISTINCT u.id)::DOUBLE "
                "FROM analytics.events e JOIN analytics.users u ON e.user_id = u.id "
                "WHERE u.cohort_month = '2026-05'"
            ),
            operand_sql={
                "first_purchase_users": (
                    "SELECT COUNT(DISTINCT e.user_id) "
                    "FILTER (WHERE e.event_name = 'first_purchase') "
                    "FROM analytics.events e JOIN analytics.users u "
                    "ON e.user_id = u.id WHERE u.cohort_month = '2026-05'"
                ),
                "cohort_signups": (
                    "SELECT COUNT(DISTINCT id) FROM analytics.users "
                    "WHERE cohort_month = '2026-05'"
                ),
            },
            adapter=adapter,
        )
        assert result.reconciles is False
        assert result.operands == {"first_purchase_users": 2.0, "cohort_signups": 4.0}
        assert result.implied_parent == 0.5
        assert abs(result.actual_parent - 2 / 3) < 1e-9
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_validation/test_reconciliation.py -v`
Expected: FAIL with `ImportError: cannot import name 'reconcile_decomposition'`

- [ ] **Step 3: Write minimal implementation**

In `src/agentic_data_contracts/validation/reconciliation.py`, extend the imports:

```python
from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass

from agentic_data_contracts.adapters.base import DatabaseAdapter
from agentic_data_contracts.semantic.base import MetricDefinition
```

Then add, after `_apply_operator`:

```python
def reconcile_decomposition(
    metric: MetricDefinition,
    *,
    parent_sql: str,
    operand_sql: Mapping[str, str],
    adapter: DatabaseAdapter,
    rel_tol: float = 1e-4,
    abs_tol: float = 0.0,
    decomposition: int = 0,
) -> ReconciliationResult:
    """Execute a metric's declared decomposition and check the identity holds.

    ``operand_sql`` maps each declared operand name to a scalar SQL query;
    ``parent_sql`` measures the parent. All queries are executed via ``adapter``.
    The result reports the numbers and whether they reconcile within tolerance;
    it never infers *why* a mismatch occurred.
    """
    if not metric.decompositions:
        raise ValueError(f"metric {metric.name!r} declares no decompositions")
    if not 0 <= decomposition < len(metric.decompositions):
        raise ValueError(
            f"decomposition index {decomposition} out of range for metric "
            f"{metric.name!r} ({len(metric.decompositions)} declared)"
        )
    decomp = metric.decompositions[decomposition]
    declared = list(decomp.operands)

    if set(operand_sql) != set(declared):
        raise ValueError(
            f"operand_sql keys {sorted(operand_sql)} do not match the declared "
            f"operands {declared} of metric {metric.name!r}"
        )

    measured: dict[str, float] = {}
    missing: list[str] = []
    for name in declared:
        value = _scalar(adapter, operand_sql[name], f"operand {name!r}")
        if value is None:
            missing.append(name)
        else:
            measured[name] = value
    actual_parent = _scalar(adapter, parent_sql, "parent")

    if missing or actual_parent is None:
        if actual_parent is None:
            missing = [*missing, "parent"]
        return ReconciliationResult(
            metric=metric.name,
            operator=decomp.operator,
            operands=measured,
            implied_parent=math.nan,
            actual_parent=math.nan if actual_parent is None else actual_parent,
            abs_diff=math.nan,
            rel_diff=math.nan,
            reconciles=False,
            rel_tol=rel_tol,
            abs_tol=abs_tol,
            reason=f"{', '.join(missing)} returned NULL",
        )

    values = [measured[name] for name in declared]

    if decomp.operator == "ratio" and values[1] == 0:
        return ReconciliationResult(
            metric=metric.name,
            operator=decomp.operator,
            operands=dict(zip(declared, values, strict=True)),
            implied_parent=math.inf,
            actual_parent=actual_parent,
            abs_diff=math.inf,
            rel_diff=math.inf,
            reconciles=False,
            rel_tol=rel_tol,
            abs_tol=abs_tol,
            reason=f"ratio denominator (operand {declared[1]!r}) is zero",
        )

    implied = _apply_operator(decomp.operator, values)
    abs_diff = abs(implied - actual_parent)
    if abs_diff == 0:
        rel_diff = 0.0
    elif actual_parent != 0:
        rel_diff = abs_diff / abs(actual_parent)
    else:
        rel_diff = math.inf
    reconciles = abs_diff <= max(abs_tol, rel_tol * abs(actual_parent))

    return ReconciliationResult(
        metric=metric.name,
        operator=decomp.operator,
        operands=dict(zip(declared, values, strict=True)),
        implied_parent=implied,
        actual_parent=actual_parent,
        abs_diff=abs_diff,
        rel_diff=rel_diff,
        reconciles=reconciles,
        rel_tol=rel_tol,
        abs_tol=abs_tol,
        reason=None if reconciles else "identity does not hold within tolerance",
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_validation/test_reconciliation.py -v`
Expected: PASS (all tests: Task 1 + Task 2 + Task 3 classes)

- [ ] **Step 5: Export from the validation package**

Edit `src/agentic_data_contracts/validation/__init__.py` — add the import and the two `__all__` entries (keep alphabetical order):

```python
from agentic_data_contracts.validation.reconciliation import (
    ReconciliationResult,
    reconcile_decomposition,
)
```

Add `"ReconciliationResult"` and `"reconcile_decomposition"` to the `__all__` list.

- [ ] **Step 6: Verify the export and run the full suite**

Run: `uv run python -c "from agentic_data_contracts.validation import reconcile_decomposition, ReconciliationResult; print('ok')"`
Expected: `ok`

Run: `uv run pytest -q`
Expected: PASS (existing suite + new tests, no regressions)

- [ ] **Step 7: Lint and type-check through prek**

Run: `prek run --all-files`
Expected: ruff + ty pass

- [ ] **Step 8: Commit**

```bash
git add src/agentic_data_contracts/validation/reconciliation.py src/agentic_data_contracts/validation/__init__.py tests/test_validation/test_reconciliation.py
git commit -m "feat: reconcile_decomposition — verify metric identities against live data"
```
