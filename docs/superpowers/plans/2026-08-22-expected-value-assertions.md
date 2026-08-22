# Expected-Value Assertions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a verified-examples corpus row carry a certified scalar answer, and add a second pass that executes the contract-compliant rows and checks the number they actually return.

**Architecture:** `validate_examples` is unchanged and still never executes a query. A new `check_example_answers` consumes the `ExampleValidationReport` it produced — so it structurally cannot be handed a non-compliant query — and for each row that is `valid` *and* declares an `expected`, it scans the SQL for a relative time window, executes it as a scalar, and compares against the certified answer within tolerance. The scalar-measurement helper `_scalar` is extracted from `reconciliation` so both contract-integrity checks share one implementation.

**Tech Stack:** Python 3.12+, sqlglot (AST scan), DuckDB (test adapter), pytest, uv, prek (ruff + ty).

**Spec:** `docs/superpowers/specs/2026-08-22-expected-value-assertions-design.md` — read it before Task 1. This plan implements it; where they disagree, the spec is right and the plan is a bug.

## Global Constraints

- Python 3.12+. Every Python command runs under `uv run`.
- Lint/format/typecheck **only** through `prek run --all-files`. Never invoke `ruff` or `ty` directly, and never name a tool version outside `.pre-commit-config.yaml`.
- TDD is mandatory: the failing test is written and *observed failing* before the implementation.
- A pygrep hook rejects `file.py:NNN` line references in source comments — name the symbol instead.
- Target version **0.44.0**. Do not bump `pyproject.toml` until Task 9.
- New public names are exported from **both** `validation/__init__.py` and the package `__init__.py`, and `__all__` lists in this repo are alphabetically sorted.
- Tolerance defaults are `rel_tol = 1e-9`, `abs_tol = 0.0` — deliberately tighter than `reconcile_decomposition`'s `1e-4`. Do not unify them.
- Every new dataclass uses `from __future__ import annotations` (already at the top of the files being modified).
- Branch: `feat/expected-value-assertions` (already checked out, spec already committed).

---

### Task 1: Extract `_scalar` into a shared module

`_scalar` currently lives in `validation/reconciliation.py`. `check_example_answers` needs the identical empty-result / NULL / non-finite / non-scalar semantics, so it moves to its own private module and both callers import it. This is a **pure move** — no behaviour change, no signature change.

`tests/test_validation/test_reconciliation.py` imports `_scalar` *from* `reconciliation`, and that import must keep working. It does, because `reconciliation` re-imports the name into its own namespace.

**Files:**
- Create: `src/agentic_data_contracts/validation/_scalar.py`
- Modify: `src/agentic_data_contracts/validation/reconciliation.py`
- Test: `tests/test_validation/test_reconciliation.py` (existing suite is the regression test)

**Interfaces:**
- Consumes: nothing.
- Produces: `_scalar(adapter: DatabaseAdapter, sql: str, label: str) -> tuple[float | None, str | None]`, importable from both `agentic_data_contracts.validation._scalar` and `agentic_data_contracts.validation.reconciliation`.

- [ ] **Step 1: Run the existing reconciliation suite to establish the baseline**

Run: `uv run pytest tests/test_validation/test_reconciliation.py -v`
Expected: PASS (all of them). Note the count — it must be identical at the end of this task.

- [ ] **Step 2: Create the new module**

Create `src/agentic_data_contracts/validation/_scalar.py`. The function body is copied verbatim from `reconciliation.py`; only the module docstring is new.

```python
"""Measure a query as a single scalar.

Shared by ``reconcile_decomposition`` and ``check_example_answers`` so the
empty-result / NULL / non-finite / non-scalar semantics have exactly one
implementation. A second copy is a second chance to get the rule wrong.
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # Deferred for the same reason reconciliation defers it: adapters.base
    # imports validation.explain, which initializes validation/__init__ before
    # adapters.base finishes defining DatabaseAdapter. Safe at runtime because
    # `from __future__ import annotations` keeps annotations unevaluated.
    from agentic_data_contracts.adapters.base import DatabaseAdapter


def _scalar(
    adapter: DatabaseAdapter, sql: str, label: str
) -> tuple[float | None, str | None]:
    """Measure ``sql`` as a single scalar.

    Returns ``(value, None)`` for a usable finite number, or ``(None, reason)``
    when the query yields no usable value: an empty result, a SQL ``NULL``, or a
    non-finite float (``NaN`` / ±``inf``, valid SQL floats distinct from
    ``NULL``). Distinguishing these lets the caller report *which* condition it
    hit rather than mislabelling every one as a NULL. Raises ``ValueError`` if
    the query is not scalar-shaped (not exactly one column, or more than one
    row).
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
        return None, f"{label} returned no rows"
    value = result.rows[0][0]
    if value is None:
        return None, f"{label} returned NULL"
    number = float(value)
    if not math.isfinite(number):
        return None, f"{label} returned a non-finite value: {number}"
    return number, None
```

- [ ] **Step 3: Delete the old copy and import the new one**

In `src/agentic_data_contracts/validation/reconciliation.py`, delete the entire `_scalar` function definition (from its `def _scalar(` line through `return number, None`) and add this import alongside the other module-level imports:

```python
from agentic_data_contracts.validation._scalar import _scalar
```

Leave everything else in the file untouched — including the `math` import, which `_apply_operator` and the `rel_diff` branches still use.

- [ ] **Step 4: Run the reconciliation suite again**

Run: `uv run pytest tests/test_validation/test_reconciliation.py -v`
Expected: PASS, with the **same test count as Step 1**. `TestScalar` still imports `_scalar` from `reconciliation` and still passes — that is the point of the re-import.

- [ ] **Step 5: Run the full suite and the linters**

Run: `uv run pytest -q && prek run --all-files`
Expected: all PASS. A pure move must not disturb anything.

- [ ] **Step 6: Commit**

```bash
git add src/agentic_data_contracts/validation/_scalar.py src/agentic_data_contracts/validation/reconciliation.py
git commit -m "refactor: _scalar moves to its own module for a second caller"
```

---

### Task 2: Assertion fields on `VerifiedExample`

Four additive optional fields, plus strict `from_dict` validation. Every existing corpus must keep parsing unchanged.

The two tolerances are **flat** fields, not a nested `tolerance` mapping — shallower YAML for corpus authors, and no new public type.

**Files:**
- Modify: `src/agentic_data_contracts/validation/examples.py`
- Test: `tests/test_validation/test_examples.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `VerifiedExample` with `expected: float | None`, `rel_tol: float | None`, `abs_tol: float | None`, `time_scoped: bool`. A module-level `_numeric(raw: Any, field_name: str, *, allow_negative: bool = True) -> float | None`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_validation/test_examples.py`, after the existing `from_dict` tests:

```python
def test_from_dict_maps_assertion_fields() -> None:
    ex = VerifiedExample.from_dict(
        {
            "sql": "SELECT 1",
            "expected": 1204338.55,
            "rel_tol": 1e-6,
            "abs_tol": 0.5,
            "time_scoped": True,
        }
    )
    assert ex.expected == 1204338.55
    assert ex.rel_tol == 1e-6
    assert ex.abs_tol == 0.5
    assert ex.time_scoped is True
    # They are first-class fields now, not unknown keys.
    assert ex.metadata == {}


def test_from_dict_defaults_assertion_fields_to_absent() -> None:
    ex = VerifiedExample.from_dict({"sql": "SELECT 1"})
    assert ex.expected is None
    assert ex.rel_tol is None
    assert ex.abs_tol is None
    assert ex.time_scoped is False


def test_from_dict_coerces_integer_expected_to_float() -> None:
    ex = VerifiedExample.from_dict({"sql": "SELECT 1", "expected": 940})
    assert ex.expected == 940.0
    assert isinstance(ex.expected, float)


def test_from_dict_rejects_boolean_expected() -> None:
    # bool is an int subclass in Python, so a naive isinstance check would let
    # `expected: true` through and assert against 1.0.
    with pytest.raises(ValueError, match="expected"):
        VerifiedExample.from_dict({"sql": "SELECT 1", "expected": True})


def test_from_dict_rejects_non_numeric_expected() -> None:
    with pytest.raises(ValueError, match="expected"):
        VerifiedExample.from_dict({"sql": "SELECT 1", "expected": "1.2M"})


def test_from_dict_rejects_non_finite_expected() -> None:
    # A YAML `.nan` / `.inf` is a malformed answer, not an assertion that can
    # ever match.
    with pytest.raises(ValueError, match="finite"):
        VerifiedExample.from_dict({"sql": "SELECT 1", "expected": float("nan")})
    with pytest.raises(ValueError, match="finite"):
        VerifiedExample.from_dict({"sql": "SELECT 1", "expected": float("inf")})


def test_from_dict_rejects_negative_tolerance() -> None:
    with pytest.raises(ValueError, match="rel_tol"):
        VerifiedExample.from_dict({"sql": "SELECT 1", "rel_tol": -1e-6})
    with pytest.raises(ValueError, match="abs_tol"):
        VerifiedExample.from_dict({"sql": "SELECT 1", "abs_tol": -0.5})


def test_from_dict_rejects_non_boolean_time_scoped() -> None:
    with pytest.raises(ValueError, match="time_scoped"):
        VerifiedExample.from_dict({"sql": "SELECT 1", "time_scoped": "yes"})
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/test_validation/test_examples.py -k "assertion_fields or expected or tolerance or time_scoped" -v`
Expected: FAIL — `TypeError: VerifiedExample.__init__() got an unexpected keyword argument` or `AttributeError`, plus the `pytest.raises` cases failing because nothing raises yet.

- [ ] **Step 3: Implement**

In `src/agentic_data_contracts/validation/examples.py`:

Add `import math` to the imports.

Extend the known-keys set:

```python
_KNOWN_KEYS = frozenset(
    {
        "sql",
        "question",
        "id",
        "principal",
        "metadata",
        "expected",
        "rel_tol",
        "abs_tol",
        "time_scoped",
    }
)
```

Add the validation helper above `VerifiedExample`:

```python
def _numeric(raw: Any, field_name: str, *, allow_negative: bool = True) -> float | None:
    """Validate one optional numeric field from an untrusted corpus row.

    ``bool`` is rejected explicitly: it is an ``int`` subclass, so
    ``expected: true`` would otherwise pass an isinstance check and silently
    assert against ``1.0``.
    """
    if raw is None:
        return None
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise ValueError(
            f"'{field_name}' must be a number, got {type(raw).__name__}"
        )
    value = float(raw)
    if not math.isfinite(value):
        raise ValueError(f"'{field_name}' must be finite, got {value}")
    if not allow_negative and value < 0:
        raise ValueError(f"'{field_name}' must be non-negative, got {value}")
    return value
```

Add the four fields to the dataclass (after `metadata`, so existing positional construction is unaffected):

```python
    expected: float | None = None
    rel_tol: float | None = None
    abs_tol: float | None = None
    time_scoped: bool = False
```

Extend the `VerifiedExample` docstring's one-liner to:

```python
    """One example to validate. Only ``sql`` is load-bearing.

    An example that sets ``expected`` is additionally an *assertion*: the
    certified answer its SQL must return, checked by ``check_example_answers``.
    ``rel_tol`` / ``abs_tol`` override the call-level tolerances for this row
    alone; ``time_scoped`` is the author's assertion that the query's time
    window is pinned, which suppresses the relative-time-window refusal.
    """
```

In `from_dict`, after the existing `metadata` handling and before the `return`, add:

```python
        time_scoped = raw.get("time_scoped", False)
        if not isinstance(time_scoped, bool):
            raise ValueError(
                f"'time_scoped' must be a boolean, got {type(time_scoped).__name__}"
            )
```

and extend the constructor call:

```python
        return cls(
            sql=raw["sql"],
            question=raw.get("question", ""),
            id=raw.get("id"),
            principal=raw.get("principal"),
            metadata=metadata,
            expected=_numeric(raw.get("expected"), "expected"),
            rel_tol=_numeric(raw.get("rel_tol"), "rel_tol", allow_negative=False),
            abs_tol=_numeric(raw.get("abs_tol"), "abs_tol", allow_negative=False),
            time_scoped=time_scoped,
        )
```

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/test_validation/test_examples.py -v`
Expected: PASS, including every pre-existing test — `test_from_dict_stashes_unknown_keys_in_metadata` still passes because the new keys are the only ones that left `metadata`.

- [ ] **Step 5: Lint**

Run: `prek run --all-files`
Expected: PASS. If ruff rewrites `isinstance(raw, (int, float))` to `isinstance(raw, int | float)`, accept its fix — the hook is the authority on style.

- [ ] **Step 6: Commit**

```bash
git add src/agentic_data_contracts/validation/examples.py tests/test_validation/test_examples.py
git commit -m "feat: VerifiedExample carries an optional certified answer"
```

---

### Task 3: Answer result and report types

Pure data — no execution, no adapter. Also extracts the `id → question → #index` label helper out of `ExampleValidationReport.summary()` so Task 5 can name rows in error messages.

**Files:**
- Modify: `src/agentic_data_contracts/validation/examples.py`
- Test: `tests/test_validation/test_examples.py`

**Interfaces:**
- Consumes: `VerifiedExample` from Task 2.
- Produces: `_DEFAULT_REL_TOL = 1e-9`; `_DEFAULT_ABS_TOL = 0.0`; module-level `_label(example: VerifiedExample, index: int) -> str`; `ExampleAnswerResult`; `ExampleAnswerReport` with `matches` / `mismatches` / `unassertable` / `errors` / `ok` / `summary()`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_validation/test_examples.py`:

```python
def _ans(status: str, **kw: object) -> ExampleAnswerResult:
    return ExampleAnswerResult(
        example=VerifiedExample(sql="SELECT 1", id=status, expected=1.0),
        status=status,
        **kw,  # type: ignore[arg-type]
    )


def test_answer_report_partitions_by_status() -> None:
    report = ExampleAnswerReport(
        results=[
            _ans("match"),
            _ans("mismatch"),
            _ans("unassertable"),
            _ans("error"),
        ]
    )
    assert [r.status for r in report.matches] == ["match"]
    assert [r.status for r in report.mismatches] == ["mismatch"]
    assert [r.status for r in report.unassertable] == ["unassertable"]
    assert [r.status for r in report.errors] == ["error"]


def test_answer_report_ok_requires_every_assertion_to_match() -> None:
    assert ExampleAnswerReport(results=[_ans("match")]).ok
    assert not ExampleAnswerReport(results=[_ans("mismatch")]).ok
    assert not ExampleAnswerReport(results=[_ans("unassertable")]).ok
    assert not ExampleAnswerReport(results=[_ans("error")]).ok
    assert not ExampleAnswerReport(results=[_ans("match"), _ans("error")]).ok


def test_empty_answer_report_is_not_ok() -> None:
    # Nothing declared an `expected`: a filter, a schema change, or an emptied
    # file dropped every assertion. That must fail rather than pass a no-op gate.
    assert not ExampleAnswerReport(results=[]).ok


def test_answer_summary_mentions_counts_and_offenders() -> None:
    report = ExampleAnswerReport(
        results=[
            _ans("match"),
            _ans("mismatch", expected=100.0, actual=98.0, abs_diff=2.0, rel_diff=0.02),
        ]
    )
    text = report.summary()
    assert "mismatch" in text
    assert "100" in text and "98" in text


def test_label_falls_back_from_id_to_question_to_index() -> None:
    assert _label(VerifiedExample(sql="s", id="ident", question="q"), 3) == "ident"
    assert _label(VerifiedExample(sql="s", question="q"), 3) == "q"
    assert _label(VerifiedExample(sql="s"), 3) == "#3"
```

Extend the import at the top of the test file:

```python
from agentic_data_contracts.validation.examples import (
    ExampleAnswerReport,
    ExampleAnswerResult,
    ExampleResult,
    ExampleValidationReport,
    VerifiedExample,
    _label,
    validate_examples,
)
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/test_validation/test_examples.py -k "answer or label" -v`
Expected: FAIL at collection — `ImportError: cannot import name 'ExampleAnswerReport'`.

- [ ] **Step 3: Implement**

In `src/agentic_data_contracts/validation/examples.py`, add the defaults near `_KNOWN_KEYS`:

```python
_DEFAULT_REL_TOL = 1e-9
_DEFAULT_ABS_TOL = 0.0
```

Extract the label helper to module level (place it just above `ExampleValidationReport`):

```python
def _label(example: VerifiedExample, index: int) -> str:
    """A row's display name: ``id`` → ``question`` → positional ``#index``.

    Shared by both reports' ``summary()`` and by the answer checker's error
    messages, so two unnamed rows never render identically.
    """
    return example.id or example.question or f"#{index}"
```

Inside `ExampleValidationReport.summary()`, delete the nested `_label` closure and change its call site from `_label(r, i)` to `_label(r.example, i)`. Update the docstring sentence that says "Iterates ``enumerate(self.results)`` once so each row can fall back to its positional index (``id → question → #index``, per the spec)" to reference the shared `_label` helper instead of a local one.

Add the new types after `ExampleValidationReport`:

```python
@dataclass
class ExampleAnswerResult:
    """The verdict for one asserted example.

    ``status`` (each result has exactly one):
      - ``"match"``        — executed and equal within tolerance.
      - ``"mismatch"``     — executed and outside tolerance. Both numbers and
                             both diffs are populated.
      - ``"unassertable"`` — the SQL uses a relative time window, so the
                             expected value decays. NOT executed.
      - ``"error"``        — no verdict was possible: not scalar-shaped, no
                             rows, NULL, non-finite, unparseable, or the
                             adapter raised.

    ``rel_tol`` / ``abs_tol`` record the tolerances actually applied to this
    row, so a mismatch names the threshold it missed.
    """

    example: VerifiedExample
    status: str
    expected: float | None = None
    actual: float | None = None
    abs_diff: float | None = None
    rel_diff: float | None = None
    rel_tol: float = _DEFAULT_REL_TOL
    abs_tol: float = _DEFAULT_ABS_TOL
    reason: str | None = None


@dataclass
class ExampleAnswerReport:
    results: list[ExampleAnswerResult]

    @property
    def matches(self) -> list[ExampleAnswerResult]:
        return [r for r in self.results if r.status == "match"]

    @property
    def mismatches(self) -> list[ExampleAnswerResult]:
        return [r for r in self.results if r.status == "mismatch"]

    @property
    def unassertable(self) -> list[ExampleAnswerResult]:
        return [r for r in self.results if r.status == "unassertable"]

    @property
    def errors(self) -> list[ExampleAnswerResult]:
        return [r for r in self.results if r.status == "error"]

    @property
    def ok(self) -> bool:
        """True only when there is ≥1 assertion and every one is ``match``.

        An **empty** report is NOT ok, mirroring ``ExampleValidationReport.ok``:
        calling the checker on a corpus where nothing declared an ``expected``
        means a filter, a schema change, or an emptied file dropped every
        assertion, and that must surface rather than pass a no-op gate. It
        does mean the second gate can only be wired into CI once at least one
        row carries an ``expected`` — add the first assertion, then the gate. A
        consumer wanting the laxer view meanwhile tests ``mismatches`` directly.
        """
        return bool(self.results) and all(r.status == "match" for r in self.results)

    def summary(self) -> str:
        """A compact markdown report, suitable for an MR comment."""
        counts = Counter(r.status for r in self.results)
        lines = [
            f"**Answer checks:** {counts['match']} match, "
            f"{counts['mismatch']} mismatch(es), "
            f"{counts['unassertable']} unassertable, "
            f"{counts['error']} error(s).",
        ]
        for i, r in enumerate(self.results):
            if r.status == "mismatch":
                lines.append(
                    f"- mismatch `{_label(r.example, i)}`: expected {r.expected}, "
                    f"actual {r.actual} (rel diff {r.rel_diff:.3g}, "
                    f"rel_tol {r.rel_tol:.3g}, abs_tol {r.abs_tol:.3g})"
                )
            elif r.status in ("unassertable", "error"):
                lines.append(f"- {r.status} `{_label(r.example, i)}`: {r.reason}")
        return "\n".join(lines)
```

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/test_validation/test_examples.py -v`
Expected: PASS, including the pre-existing `test_summary_mentions_counts_and_offenders` — the `_label` extraction is behaviour-preserving.

- [ ] **Step 5: Lint and commit**

```bash
prek run --all-files
git add src/agentic_data_contracts/validation/examples.py tests/test_validation/test_examples.py
git commit -m "feat: answer result and report types for corpus assertions"
```

---

### Task 4: The comparison kernel

Pure arithmetic, no I/O. Isolated because it is where the two subtle decisions live: the zero-reference guard and the anchoring choice.

**Files:**
- Modify: `src/agentic_data_contracts/validation/examples.py`
- Test: `tests/test_validation/test_examples.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `_compare(actual: float, expected: float, rel_tol: float, abs_tol: float) -> tuple[float, float, bool]` returning `(abs_diff, rel_diff, matched)`.

- [ ] **Step 1: Write the failing tests**

```python
class TestCompare:
    def test_exact_match(self) -> None:
        abs_diff, rel_diff, matched = _compare(100.0, 100.0, 1e-9, 0.0)
        assert (abs_diff, rel_diff, matched) == (0.0, 0.0, True)

    def test_outside_tolerance_is_mismatch(self) -> None:
        abs_diff, rel_diff, matched = _compare(98.0, 100.0, 1e-9, 0.0)
        assert abs_diff == 2.0
        assert rel_diff == pytest.approx(0.02)
        assert not matched

    def test_within_relative_tolerance(self) -> None:
        _, _, matched = _compare(100.05, 100.0, 1e-3, 0.0)
        assert matched

    def test_within_absolute_tolerance(self) -> None:
        _, _, matched = _compare(100.5, 100.0, 0.0, 1.0)
        assert matched

    def test_zero_expected_exact_match_does_not_divide_by_zero(self) -> None:
        # A certified answer of zero is legitimate ("how many failed orders in
        # Q1? None") and must not raise or report a meaningless inf.
        abs_diff, rel_diff, matched = _compare(0.0, 0.0, 1e-9, 0.0)
        assert (abs_diff, rel_diff, matched) == (0.0, 0.0, True)

    def test_zero_expected_near_miss_reports_infinite_rel_diff(self) -> None:
        abs_diff, rel_diff, matched = _compare(0.5, 0.0, 1e-9, 0.0)
        assert abs_diff == 0.5
        assert rel_diff == math.inf
        assert not matched

    def test_zero_expected_rescued_by_abs_tol(self) -> None:
        # The relative term vanishes at expected == 0, so abs_tol is the only
        # way to allow any slack there.
        _, _, matched = _compare(0.5, 0.0, 1e-9, 1.0)
        assert matched

    def test_relative_term_is_anchored_on_expected_not_actual(self) -> None:
        # expected=100, actual=200, rel_tol=0.75. Anchored on expected the
        # threshold is 75 and the diff of 100 is a mismatch; anchored on actual
        # (or on max(|a|,|b|), as math.isclose does) it would be 150 and pass.
        _, _, matched = _compare(200.0, 100.0, 0.75, 0.0)
        assert not matched
```

Add `import math` to the test file's imports if absent, and extend the examples import with `_compare`.

- [ ] **Step 2: Run to verify they fail**

Run: `uv run pytest tests/test_validation/test_examples.py::TestCompare -v`
Expected: FAIL at collection — `ImportError: cannot import name '_compare'`.

- [ ] **Step 3: Implement**

Add to `src/agentic_data_contracts/validation/examples.py`:

```python
def _compare(
    actual: float, expected: float, rel_tol: float, abs_tol: float
) -> tuple[float, float, bool]:
    """Compare a measured value against a certified answer.

    Returns ``(abs_diff, rel_diff, matched)``.

    ``rel_diff`` is guarded against a zero reference — a certified answer of
    zero is legitimate, and dividing by it would raise or report a meaningless
    ``inf`` for an exact match. The three-branch form is the same one
    ``reconcile_decomposition`` uses for a zero parent.

    The relative term is anchored on ``expected``, deliberately unlike
    ``reconcile_decomposition`` (which compares two measurements and has no
    privileged side) and unlike ``math.isclose`` (which anchors on the larger
    magnitude). An assertion *has* a reference: the certified answer is the
    fixed point and the query result is what varies against it. Anchoring on
    ``expected`` keeps the tolerance's meaning stable — "within 0.1% of the
    certified number" — however far the query has drifted.
    """
    abs_diff = abs(actual - expected)
    if abs_diff == 0:
        rel_diff = 0.0
    elif expected != 0:
        rel_diff = abs_diff / abs(expected)
    else:
        rel_diff = math.inf
    matched = abs_diff <= max(abs_tol, rel_tol * abs(expected))
    return abs_diff, rel_diff, matched
```

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/test_validation/test_examples.py::TestCompare -v`
Expected: PASS (8 tests).

- [ ] **Step 5: Lint and commit**

```bash
prek run --all-files
git add src/agentic_data_contracts/validation/examples.py tests/test_validation/test_examples.py
git commit -m "feat: comparison kernel for certified answers"
```

---

### Task 5: `check_example_answers` — filtering, execution, tolerance resolution

The core pass. Time-scope scanning (Task 6) and the error paths (Task 7) are layered on next.

The filtering rule is the security-relevant part: **only rows that are `status == "valid"` and declare an `expected` are executed.** A row that violated the tenant-filter rule is precisely the query that must never be run against a warehouse.

**Files:**
- Modify: `src/agentic_data_contracts/validation/examples.py`
- Test: `tests/test_validation/test_examples.py`

**Interfaces:**
- Consumes: `_compare`, `_label`, `_scalar`, `ExampleAnswerResult`, `ExampleAnswerReport`, `ExampleValidationReport`.
- Produces: `check_example_answers(report: ExampleValidationReport, *, adapter: DatabaseAdapter, dialect: str | None = None, sql_normalizer: SqlNormalizer | None = None, rel_tol: float = 1e-9, abs_tol: float = 0.0) -> ExampleAnswerReport`.

- [ ] **Step 1: Write the failing tests**

Add the spy adapter and the tests. The spy records every SQL string it is asked to execute, which is how the never-execute guarantees are asserted directly rather than inferred from a status.

```python
_NO_ROWS = object()
_TWO_COLS = object()


class SpyAdapter:
    """A DatabaseAdapter that returns canned scalars and records every call."""

    def __init__(self, values: dict[str, object] | None = None) -> None:
        self.values = values or {}
        self.calls: list[str] = []

    def execute(self, sql: str) -> QueryResult:
        self.calls.append(sql)
        if sql not in self.values:
            raise AssertionError(f"SpyAdapter has no canned value for: {sql}")
        value = self.values[sql]
        if isinstance(value, Exception):
            raise value
        if value is _NO_ROWS:
            return QueryResult(columns=["v"], rows=[])
        if value is _TWO_COLS:
            return QueryResult(columns=["a", "b"], rows=[(1, 2)])
        return QueryResult(columns=["v"], rows=[(value,)])

    def explain(self, sql: str) -> ExplainResult:
        return ExplainResult(
            estimated_cost_usd=None, estimated_rows=1, schema_valid=True
        )

    def describe_table(self, schema: str, table: str) -> TableSchema:
        return TableSchema(columns=[])

    def list_tables(self, schema: str) -> list[str]:
        return []

    @property
    def dialect(self) -> str:
        return "duckdb"


_SUM_SQL = "SELECT SUM(amount) FROM analytics.orders WHERE tenant_id = 'acme'"


def _asserted(sql: str, expected: float, **kw: object) -> VerifiedExample:
    return VerifiedExample(sql=sql, expected=expected, **kw)  # type: ignore[arg-type]


def test_matching_assertion_is_a_match(contract: DataContract) -> None:
    adapter = SpyAdapter({_SUM_SQL: 1204338.55})
    report = validate_examples([_asserted(_SUM_SQL, 1204338.55)], contract)
    answers = check_example_answers(report, adapter=adapter)
    assert answers.ok
    r = answers.results[0]
    assert r.status == "match"
    assert r.expected == 1204338.55
    assert r.actual == 1204338.55
    assert adapter.calls == [_SUM_SQL]


def test_wrong_number_is_a_mismatch(contract: DataContract) -> None:
    adapter = SpyAdapter({_SUM_SQL: 1198000.0})
    report = validate_examples([_asserted(_SUM_SQL, 1204338.55)], contract)
    answers = check_example_answers(report, adapter=adapter)
    assert not answers.ok
    r = answers.results[0]
    assert r.status == "mismatch"
    assert r.expected == 1204338.55
    assert r.actual == 1198000.0
    assert r.abs_diff is not None and r.abs_diff > 0
    assert r.rel_diff is not None and r.rel_diff > 0


def test_violation_row_is_never_executed(contract: DataContract) -> None:
    # The security-relevant guarantee: a query that failed the contract must
    # not be run against the warehouse to see what it returns.
    bad = _asserted("SELECT SUM(amount) FROM raw.payments WHERE tenant_id = 'x'", 1.0)
    adapter = SpyAdapter()
    report = validate_examples([bad], contract)
    assert report.results[0].status == "violation"
    answers = check_example_answers(report, adapter=adapter)
    assert answers.results == []
    assert adapter.calls == []


def test_unchecked_row_is_never_executed(contract: DataContract) -> None:
    adapter = SpyAdapter()
    report = validate_examples([_asserted("SELECT * FROM (", 1.0)], contract)
    assert report.results[0].status == "unchecked"
    answers = check_example_answers(report, adapter=adapter)
    assert answers.results == []
    assert adapter.calls == []


def test_unverified_row_is_never_executed(contract: DataContract) -> None:
    # Decision-B: the engine planned it but contract policy was never statically
    # checked, so it is not vouched-valid and must not be executed either.
    explain = FakeExplainAdapter(
        ExplainResult(estimated_cost_usd=None, estimated_rows=1, schema_valid=True)
    )
    adapter = SpyAdapter()
    report = validate_examples(
        [_asserted("SELECT * FROM (", 1.0)], contract, explain_adapter=explain
    )
    assert report.results[0].status == "unverified"
    answers = check_example_answers(report, adapter=adapter)
    assert answers.results == []
    assert adapter.calls == []


def test_a_decimal_result_compares_as_a_number(contract: DataContract) -> None:
    # A money column comes back as DECIMAL from DuckDB/Postgres, not float.
    # _scalar's float() coercion handles it; this pins that it keeps doing so.
    adapter = SpyAdapter({_SUM_SQL: Decimal("10700.00")})
    answers = check_example_answers(
        validate_examples([_asserted(_SUM_SQL, 10700.0)], contract), adapter=adapter
    )
    assert answers.results[0].status == "match"
    assert answers.results[0].actual == 10700.0


def test_row_without_expected_produces_no_result(contract: DataContract) -> None:
    adapter = SpyAdapter()
    report = validate_examples([VerifiedExample(sql=_SUM_SQL)], contract)
    answers = check_example_answers(report, adapter=adapter)
    assert answers.results == []
    assert adapter.calls == []


def test_per_example_tolerance_beats_the_call_level_default(
    contract: DataContract,
) -> None:
    # An answer certified from a dashboard rounded to cents against a
    # full-precision SUM: rescued by the row's own rel_tol.
    adapter = SpyAdapter({_SUM_SQL: 1204338.5512})
    ex = _asserted(_SUM_SQL, 1204338.55, rel_tol=1e-6)
    answers = check_example_answers(
        validate_examples([ex], contract), adapter=adapter
    )
    assert answers.results[0].status == "match"
    assert answers.results[0].rel_tol == 1e-6


def test_call_level_tolerance_applies_when_the_row_sets_none(
    contract: DataContract,
) -> None:
    adapter = SpyAdapter({_SUM_SQL: 1204338.5512})
    report = validate_examples([_asserted(_SUM_SQL, 1204338.55)], contract)
    assert check_example_answers(report, adapter=adapter).results[0].status == (
        "mismatch"
    )
    report2 = validate_examples([_asserted(_SUM_SQL, 1204338.55)], contract)
    loose = check_example_answers(report2, adapter=adapter, rel_tol=1e-6)
    assert loose.results[0].status == "match"


def test_a_row_may_override_one_tolerance_without_the_other(
    contract: DataContract,
) -> None:
    adapter = SpyAdapter({_SUM_SQL: 100.5})
    ex = _asserted(_SUM_SQL, 100.0, abs_tol=1.0)
    answers = check_example_answers(
        validate_examples([ex], contract), adapter=adapter, rel_tol=1e-3
    )
    r = answers.results[0]
    assert r.status == "match"
    assert r.abs_tol == 1.0  # from the row
    assert r.rel_tol == 1e-3  # from the call
```

Extend the test imports:

```python
from decimal import Decimal

from agentic_data_contracts.adapters.base import QueryResult, TableSchema
from agentic_data_contracts.validation.examples import (
    ...,
    _compare,
    check_example_answers,
)
```

- [ ] **Step 2: Run to verify they fail**

Run: `uv run pytest tests/test_validation/test_examples.py -k "assertion or mismatch or executed or tolerance or expected" -v`
Expected: FAIL at collection — `ImportError: cannot import name 'check_example_answers'`.

- [ ] **Step 3: Implement**

In `src/agentic_data_contracts/validation/examples.py`, extend the `TYPE_CHECKING` block:

```python
if TYPE_CHECKING:
    from agentic_data_contracts.adapters.base import DatabaseAdapter
    from agentic_data_contracts.semantic.base import SemanticSource
```

and add the runtime import alongside the existing ones:

```python
from agentic_data_contracts.validation._scalar import _scalar
```

Add the function at the end of the module:

```python
def check_example_answers(
    report: ExampleValidationReport,
    *,
    adapter: DatabaseAdapter,
    dialect: str | None = None,
    sql_normalizer: SqlNormalizer | None = None,
    rel_tol: float = _DEFAULT_REL_TOL,
    abs_tol: float = _DEFAULT_ABS_TOL,
) -> ExampleAnswerReport:
    """Execute each *asserted*, contract-compliant example and check its answer.

    Takes the report from ``validate_examples`` rather than raw examples, and
    that is the load-bearing choice: an example that failed contract validation
    must never be executed, and consuming the report makes that ordering a
    property of the signature rather than a rule in a docstring. A row is
    executed only when it is ``status == "valid"`` AND declares an ``expected``;
    everything else produces no result at all (it is already accounted for by
    ``ExampleValidationReport``).

    ``validate_examples`` keeps its own property of never executing a query —
    it plans, via ``ExplainAdapter``, and nothing more. The execute-capable
    ``DatabaseAdapter`` enters only here.

    ``sql_normalizer`` must be the same value passed to ``validate_examples``:
    a corpus whose SQL only parses after normalization reached ``valid``
    through the normalizer. ``dialect`` defaults to ``adapter.dialect``, since
    ``DatabaseAdapter`` exposes one (unlike the bare ``ExplainAdapter`` that
    forces ``validate_examples`` to be told); pass it explicitly only when the
    corpus is authored in a different dialect than the adapter speaks.
    """
    effective_dialect = dialect if dialect is not None else adapter.dialect
    results: list[ExampleAnswerResult] = []
    for index, row in enumerate(report.results):
        example = row.example
        if row.status != "valid" or example.expected is None:
            continue
        results.append(
            _check_one(
                example,
                _label(example, index),
                adapter=adapter,
                dialect=effective_dialect,
                sql_normalizer=sql_normalizer,
                rel_tol=example.rel_tol if example.rel_tol is not None else rel_tol,
                abs_tol=example.abs_tol if example.abs_tol is not None else abs_tol,
            )
        )
    return ExampleAnswerReport(results=results)


def _check_one(
    example: VerifiedExample,
    label: str,
    *,
    adapter: DatabaseAdapter,
    dialect: str | None,
    sql_normalizer: SqlNormalizer | None,
    rel_tol: float,
    abs_tol: float,
) -> ExampleAnswerResult:
    expected = example.expected
    assert expected is not None  # guarded by the caller's filter

    def _make(status: str, **kw: Any) -> ExampleAnswerResult:
        return ExampleAnswerResult(
            example=example,
            status=status,
            expected=expected,
            rel_tol=rel_tol,
            abs_tol=abs_tol,
            **kw,
        )

    actual, reason = _scalar(adapter, example.sql, label)
    if actual is None:
        return _make("error", reason=reason)
    diff, rel_diff, matched = _compare(actual, expected, rel_tol, abs_tol)
    return _make(
        "match" if matched else "mismatch",
        actual=actual,
        abs_diff=diff,
        rel_diff=rel_diff,
        reason=None if matched else "answer differs from the certified value",
    )
```

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/test_validation/test_examples.py -v`
Expected: PASS.

- [ ] **Step 5: Lint and commit**

```bash
prek run --all-files
git add src/agentic_data_contracts/validation/examples.py tests/test_validation/test_examples.py
git commit -m "feat: check_example_answers executes compliant assertions"
```

---

### Task 6: Time-scope refusal

An expected value attached to `WHERE d > CURRENT_DATE - 30` decays: green today, red in 30 days, for no real reason. Such a row is refused, not executed.

Verified against sqlglot: `NOW()` (postgres), `GETDATE()` (tsql), `SYSDATE` (oracle) and `CURRENT_TIMESTAMP()` (bigquery) all parse to `exp.CurrentTimestamp`; `CURRENT_DATE` (duckdb) and `CURRENT_DATE()` (snowflake) to `exp.CurrentDate`.

**Files:**
- Modify: `src/agentic_data_contracts/validation/examples.py`
- Test: `tests/test_validation/test_examples.py`

**Interfaces:**
- Consumes: everything from Task 5.
- Produces: `_TIME_FUNCS`; `_relative_time_node(statement: exp.Expression) -> str | None`. `_check_one` gains the scan before execution.

- [ ] **Step 1: Write the failing tests**

```python
_ROLLING_SQL = (
    "SELECT SUM(amount) FROM analytics.orders "
    "WHERE tenant_id = 'acme' AND created_at >= CURRENT_DATE - 30"
)


def test_relative_time_window_is_unassertable_and_never_executed(
    contract: DataContract,
) -> None:
    adapter = SpyAdapter()
    report = validate_examples([_asserted(_ROLLING_SQL, 88120.0)], contract)
    assert report.results[0].status == "valid"  # it is a fine query, just not pinnable
    answers = check_example_answers(report, adapter=adapter)
    r = answers.results[0]
    assert r.status == "unassertable"
    assert r.reason is not None and "CurrentDate" in r.reason
    assert adapter.calls == []  # the guarantee, asserted directly
    assert not answers.ok


def test_time_scoped_flag_permits_a_relative_window(contract: DataContract) -> None:
    adapter = SpyAdapter({_ROLLING_SQL: 88120.0})
    ex = _asserted(_ROLLING_SQL, 88120.0, time_scoped=True)
    answers = check_example_answers(
        validate_examples([ex], contract), adapter=adapter
    )
    assert answers.results[0].status == "match"
    assert adapter.calls == [_ROLLING_SQL]


@pytest.mark.parametrize(
    ("fragment", "node"),
    [
        ("CURRENT_DATE", "CurrentDate"),
        ("CURRENT_TIMESTAMP", "CurrentTimestamp"),
        ("NOW()", "CurrentTimestamp"),
    ],
)
def test_each_relative_time_spelling_is_detected(
    contract: DataContract, fragment: str, node: str
) -> None:
    sql = (
        "SELECT SUM(amount) FROM analytics.orders "
        f"WHERE tenant_id = 'acme' AND created_at >= {fragment}"
    )
    adapter = SpyAdapter()
    answers = check_example_answers(
        validate_examples([_asserted(sql, 1.0)], contract), adapter=adapter
    )
    assert answers.results[0].status == "unassertable"
    assert node in (answers.results[0].reason or "")
    assert adapter.calls == []


def test_dialect_defaults_to_the_adapters(contract: DataContract) -> None:
    adapter = SpyAdapter({_SUM_SQL: 1.0})
    answers = check_example_answers(
        validate_examples([_asserted(_SUM_SQL, 1.0)], contract), adapter=adapter
    )
    assert answers.results[0].status == "match"  # parsed under duckdb, no crash


def test_explicit_dialect_wins_over_the_adapters(contract: DataContract) -> None:
    adapter = SpyAdapter({_SUM_SQL: 1.0})
    answers = check_example_answers(
        validate_examples([_asserted(_SUM_SQL, 1.0)], contract),
        adapter=adapter,
        dialect="postgres",
    )
    assert answers.results[0].status == "match"
```

- [ ] **Step 2: Run to verify they fail**

Run: `uv run pytest tests/test_validation/test_examples.py -k "time or dialect" -v`
Expected: FAIL — the rolling-window rows come back `match`/`error` instead of `unassertable`, and `adapter.calls` is non-empty.

- [ ] **Step 3: Implement**

Add the imports at the top of `examples.py`:

```python
import sqlglot
from sqlglot import exp
```

Add above `check_example_answers`:

```python
# Where every dialect's relative-time spelling lands after sqlglot parses it:
# NOW() / GETDATE() / SYSDATE / CURRENT_TIMESTAMP() all become CurrentTimestamp,
# CURRENT_DATE / CURRENT_DATE() become CurrentDate. CurrentTime and
# CurrentDatetime are covered for completeness.
_TIME_FUNCS = (
    exp.CurrentDate,
    exp.CurrentTimestamp,
    exp.CurrentTime,
    exp.CurrentDatetime,
)


def _relative_time_node(statement: exp.Expression) -> str | None:
    """Name the first non-deterministic time function in *statement*, if any.

    An expected value attached to a relative window decays: correct today,
    wrong in a month, for no reason the corpus author did anything about. Such
    a row is refused rather than executed.
    """
    node = statement.find(*_TIME_FUNCS)
    return type(node).__name__ if node is not None else None
```

In `_check_one`, insert the scan between the `_make` helper and the `_scalar` call:

```python
    if not example.time_scoped:
        normalized = (
            sql_normalizer.normalize_sql(example.sql)
            if sql_normalizer
            else example.sql
        )
        statement = sqlglot.parse_one(normalized, dialect=dialect)
        found = _relative_time_node(statement)
        if found is not None:
            return _make(
                "unassertable",
                reason=(
                    f"relative time window ({found}) — the expected value "
                    "decays; pin the window or set time_scoped: true"
                ),
            )
```

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/test_validation/test_examples.py -v`
Expected: PASS.

- [ ] **Step 5: Lint and commit**

```bash
prek run --all-files
git add src/agentic_data_contracts/validation/examples.py tests/test_validation/test_examples.py
git commit -m "feat: refuse assertions over a relative time window"
```

---

### Task 7: Error handling and batch resilience

One malformed row must never kill validation of the rest of the corpus — the same posture `validate_examples` already takes with its per-example guard.

Note the ordering guarantee this task must preserve: a row whose SQL cannot be re-parsed degrades to `error` **without executing**. An unparseable statement cannot be cleared of a relative time window, so it does not get to run.

**Files:**
- Modify: `src/agentic_data_contracts/validation/examples.py`
- Test: `tests/test_validation/test_examples.py`

**Interfaces:**
- Consumes: everything from Task 6.
- Produces: no new names — `_check_one` calls are wrapped in a per-example guard inside `check_example_answers`.

- [ ] **Step 1: Write the failing tests**

```python
def test_non_scalar_sql_is_an_error(contract: DataContract) -> None:
    sql = (
        "SELECT SUM(amount) AS a, COUNT(id) AS b "
        "FROM analytics.orders WHERE tenant_id = 'acme'"
    )
    adapter = SpyAdapter({sql: _TWO_COLS})
    answers = check_example_answers(
        validate_examples([_asserted(sql, 1.0)], contract), adapter=adapter
    )
    r = answers.results[0]
    assert r.status == "error"
    assert r.reason is not None and "exactly one column" in r.reason


@pytest.mark.parametrize(
    ("value", "fragment"),
    [(_NO_ROWS, "no rows"), (None, "NULL"), (float("nan"), "non-finite")],
)
def test_unusable_scalar_is_an_error(
    contract: DataContract, value: object, fragment: str
) -> None:
    adapter = SpyAdapter({_SUM_SQL: value})
    answers = check_example_answers(
        validate_examples([_asserted(_SUM_SQL, 1.0)], contract), adapter=adapter
    )
    r = answers.results[0]
    assert r.status == "error"
    assert r.reason is not None and fragment in r.reason


def test_a_raising_adapter_degrades_only_its_own_row(contract: DataContract) -> None:
    other = "SELECT COUNT(id) FROM analytics.orders WHERE tenant_id = 'acme'"
    adapter = SpyAdapter(
        {_SUM_SQL: RuntimeError("connection reset"), other: 42.0}
    )
    report = validate_examples(
        [_asserted(_SUM_SQL, 1.0, id="boom"), _asserted(other, 42.0, id="fine")],
        contract,
    )
    answers = check_example_answers(report, adapter=adapter)
    assert [r.status for r in answers.results] == ["error", "match"]
    assert "connection reset" in (answers.results[0].reason or "")


def test_unparseable_sql_is_an_error_and_is_never_executed(
    contract: DataContract,
) -> None:
    # A normalizer or dialect mismatch between the two passes. An unparseable
    # statement cannot be cleared of a relative time window, so it must not run.
    class BrokenNormalizer:
        def normalize_sql(self, sql: str) -> str:
            return "SELECT * FROM ("

    adapter = SpyAdapter()
    answers = check_example_answers(
        validate_examples([_asserted(_SUM_SQL, 1.0)], contract),
        adapter=adapter,
        sql_normalizer=BrokenNormalizer(),
    )
    assert answers.results[0].status == "error"
    assert adapter.calls == []


def test_results_preserve_report_order(contract: DataContract) -> None:
    other = "SELECT COUNT(id) FROM analytics.orders WHERE tenant_id = 'acme'"
    adapter = SpyAdapter({_SUM_SQL: 1.0, other: 2.0})
    report = validate_examples(
        [
            _asserted(_SUM_SQL, 1.0, id="a"),
            VerifiedExample(sql=other, id="skipped"),  # no expected
            _asserted(other, 2.0, id="c"),
        ],
        contract,
    )
    answers = check_example_answers(report, adapter=adapter)
    assert [r.example.id for r in answers.results] == ["a", "c"]
```

- [ ] **Step 2: Run to verify they fail**

Run: `uv run pytest tests/test_validation/test_examples.py -k "error or raising or unparseable or preserve_report" -v`
Expected: FAIL — the non-scalar and raising cases propagate `ValueError` / `RuntimeError` out of `check_example_answers` instead of degrading.

- [ ] **Step 3: Implement**

In `check_example_answers`, wrap the `_check_one` call:

```python
        try:
            results.append(
                _check_one(
                    example,
                    _label(example, index),
                    adapter=adapter,
                    dialect=effective_dialect,
                    sql_normalizer=sql_normalizer,
                    rel_tol=example.rel_tol if example.rel_tol is not None else rel_tol,
                    abs_tol=example.abs_tol if example.abs_tol is not None else abs_tol,
                )
            )
        except Exception as exc:  # noqa: BLE001 — batch resilience
            # A non-scalar result (_scalar raises), an unparseable statement, a
            # driver error, a timeout: this row gets no verdict, and the rest of
            # the corpus still gets one. Mirrors validate_examples' own guard.
            results.append(
                ExampleAnswerResult(
                    example=example,
                    status="error",
                    expected=example.expected,
                    rel_tol=example.rel_tol if example.rel_tol is not None else rel_tol,
                    abs_tol=example.abs_tol if example.abs_tol is not None else abs_tol,
                    reason=f"answer check error: {exc}",
                )
            )
```

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/test_validation/test_examples.py -v`
Expected: PASS.

- [ ] **Step 5: Run the full suite**

Run: `uv run pytest -q && prek run --all-files`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add src/agentic_data_contracts/validation/examples.py tests/test_validation/test_examples.py
git commit -m "feat: one bad assertion never aborts the corpus check"
```

---

### Task 8: Exports

**Files:**
- Modify: `src/agentic_data_contracts/validation/__init__.py`
- Modify: `src/agentic_data_contracts/__init__.py`
- Test: `tests/test_public_api.py`

**Interfaces:**
- Consumes: `ExampleAnswerResult`, `ExampleAnswerReport`, `check_example_answers`.
- Produces: those three names importable from `agentic_data_contracts` and `agentic_data_contracts.validation`.

- [ ] **Step 1: Write the failing test**

`tests/test_public_api.py` uses function-local imports with `assert X is not None`. Follow that shape — append:

```python
def test_answer_checking_exports() -> None:
    """v0.44.0 promises these three names at the top level and in .validation."""
    from agentic_data_contracts import (
        ExampleAnswerReport,
        ExampleAnswerResult,
        check_example_answers,
    )
    from agentic_data_contracts.validation import (
        ExampleAnswerReport as VExampleAnswerReport,
    )
    from agentic_data_contracts.validation import (
        ExampleAnswerResult as VExampleAnswerResult,
    )
    from agentic_data_contracts.validation import (
        check_example_answers as v_check_example_answers,
    )

    assert ExampleAnswerReport is VExampleAnswerReport
    assert ExampleAnswerResult is VExampleAnswerResult
    assert check_example_answers is v_check_example_answers
    assert callable(check_example_answers)
    # The empty-is-not-ok rule is part of the published contract.
    assert ExampleAnswerReport(results=[]).ok is False
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_public_api.py -v`
Expected: FAIL — `ImportError: cannot import name 'ExampleAnswerReport'`.

- [ ] **Step 3: Implement**

In `src/agentic_data_contracts/validation/__init__.py`, extend the examples import and `__all__` (keeping `__all__` alphabetically sorted):

```python
from agentic_data_contracts.validation.examples import (
    ExampleAnswerReport,
    ExampleAnswerResult,
    ExampleResult,
    ExampleValidationReport,
    VerifiedExample,
    check_example_answers,
    validate_examples,
)
```

Add `"ExampleAnswerReport"`, `"ExampleAnswerResult"` and `"check_example_answers"` to `__all__` in sorted position.

Make the matching addition in `src/agentic_data_contracts/__init__.py`, following exactly how that file already re-exports `validate_examples` and `VerifiedExample`.

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/test_public_api.py -v && prek run --all-files`
Expected: PASS. `ty` will catch a name added to `__all__` but not imported.

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/validation/__init__.py src/agentic_data_contracts/__init__.py tests/test_public_api.py
git commit -m "feat: export the answer-checking API"
```

---

### Task 9: Docs, demo, changelog, version

**Files:**
- Modify: `README.md` (the "Validating a verified-examples corpus" section)
- Modify: `examples/revenue_agent/verified_examples.yml`
- Modify: `examples/revenue_agent/verify_examples.py`
- Modify: `CHANGELOG.md`
- Modify: `pyproject.toml`

**Interfaces:**
- Consumes: the full public API from Task 8.
- Produces: nothing code-facing.

- [ ] **Step 1: Extend the demo corpus**

The demo DB is `sample_data.duckdb` at the repo root, built by `examples/revenue_agent/setup_db.py`. `analytics.orders` is `(id INTEGER, customer_id INTEGER, amount DECIMAL(10,2), status VARCHAR, tenant_id VARCHAR, created_at DATE)`.

Re-confirm the true value before writing it into the corpus — the fixture data can change:

```bash
uv run python examples/revenue_agent/setup_db.py
uv run python -c "
from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
print(DuckDBAdapter('sample_data.duckdb').execute(
  \"SELECT SUM(amount) FROM analytics.orders WHERE tenant_id = 'acme' AND status = 'completed'\"
).rows)"
```

At the time of writing that returns `Decimal('10700.00')`.

Append three entries to `examples/revenue_agent/verified_examples.yml`, following its existing comment style (each entry introduced by a numbered comment explaining what it demonstrates):

```yaml
# 5. Asserted and correct — the certified answer matches what the query
#    returns, so the second pass reports `match`. Note the answer comes back
#    as a DECIMAL from DuckDB; the checker coerces it to a float.
- id: acme-completed-revenue
  question: "total completed revenue for acme"
  sql: >
    SELECT SUM(amount) FROM analytics.orders
    WHERE tenant_id = 'acme' AND status = 'completed'
  expected: 10700.00
  type: sql
  verified_by: data-eng-finance
  last_verified: 2026-08-22

# 6. Asserted and WRONG — contract-compliant SQL that returns the wrong
#    number. `validate_examples` calls this `valid`; only the answer check
#    catches it. This is the whole point of the second pass.
- id: acme-completed-revenue-stale
  question: "total completed revenue for acme (stale certified answer)"
  sql: >
    SELECT SUM(amount) FROM analytics.orders
    WHERE tenant_id = 'acme' AND status = 'completed'
  expected: 9800.00
  type: sql
  verified_by: data-eng-finance
  last_verified: 2026-01-15

# 7. Asserted over a ROLLING window — the expected value decays, so the
#    checker refuses it as `unassertable` and never runs it. Pin the window
#    (or set time_scoped: true) to make it a real assertion.
- id: acme-revenue-last-30-days
  question: "acme revenue over the last 30 days"
  sql: >
    SELECT SUM(amount) FROM analytics.orders
    WHERE tenant_id = 'acme' AND created_at >= CURRENT_DATE - 30
  expected: 4200.00
  type: sql
  verified_by: data-eng-finance
  last_verified: 2026-08-22
```

If Step 3's run shows entry 5 as a `mismatch`, the fixture data moved — update `expected` to the value the query actually returns, not the other way round.

- [ ] **Step 2: Extend the demo script**

In `examples/revenue_agent/verify_examples.py`, add the second pass after the existing validation pass, and extend the module docstring with a paragraph on what assertions add (compliance vs correctness). It must **report and exit zero** — the deliberate mismatch is the point of the demo and must not fail CI:

```python
answers = check_example_answers(report, adapter=adapter)
print(answers.summary())
```

- [ ] **Step 3: Run the demo**

Run: `uv run python examples/revenue_agent/verify_examples.py; echo "exit=$?"`
Expected: prints both summaries, shows one `match`, one `mismatch`, one `unassertable`, and `exit=0`.

- [ ] **Step 4: Extend the README**

In the "Validating a verified-examples corpus" section, add a subsection covering: the YAML shape (`expected`, `rel_tol`, `abs_tol`, `time_scoped`); the composed CI gate (`if not (report.ok and answers.ok)`); why a violation is never executed; why the default tolerance is tight and when to widen it; and the `expected: 0` consequence — the relative term vanishes at zero, so a zero-valued assertion matches only exactly unless the author sets an `abs_tol`. Also update the "Validate a whole corpus" bullet in the Highlights list to mention answers, not just contract compliance.

- [ ] **Step 5: Changelog and version**

Add a `## [0.44.0] - 2026-08-22` section at the top of `CHANGELOG.md` under an `### Added` heading, matching the existing entries' depth — they explain the *why* and the failure mode, not just the API. Cover: the gap (contract-compliant SQL that returns the wrong number passed as `valid`); the report-consuming signature and why; the time-scope refusal; and the tolerance anchoring choice.

Bump `version` in `pyproject.toml` to `0.44.0`.

- [ ] **Step 6: Full verification**

Run: `uv run pytest -q && prek run --all-files && uv run python examples/revenue_agent/verify_examples.py`
Expected: all PASS, demo exits zero.

- [ ] **Step 7: Commit**

```bash
git add README.md CHANGELOG.md pyproject.toml examples/revenue_agent/
git commit -m "docs: expected-value assertions in the README, demo, and changelog"
```

---

## Verification

Before opening the PR:

```bash
uv run pytest -q
prek run --all-files
uv run python examples/revenue_agent/verify_examples.py
```

All three must pass, and the demo must exit zero. Then confirm against the spec's Testing section that every listed case has a test.
