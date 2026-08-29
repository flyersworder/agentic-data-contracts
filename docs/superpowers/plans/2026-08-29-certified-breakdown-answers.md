# Certified Breakdown Answers (`expected_rows`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a verified-examples corpus certify a breakdown answer — a group-by, a top-N, any multi-row result — so `check_example_answers` catches a query that returns the right total with the wrong split.

**Architecture:** A new `expected_rows` sibling field on `VerifiedExample` (never a widened `expected`, so `_scalar`'s one-column/one-row rule stays enforceable), compared by a new pure module `validation/_rows.py`. Rows are paired by key — the non-numeric cells — and values compared through the existing tolerance rule. `_compare` moves to its own module first so `_rows` can use it without a circular import.

**Tech Stack:** Python 3.12+, `uv`, `pytest`, `sqlglot`, `ruff` + `ty` via `prek`. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-08-29-certified-breakdown-answers-design.md`

## Global Constraints

- Run everything through `uv run`. Lint with `prek run --all-files`, never a bare `ruff`/`ty` — the hook `rev`s in `.pre-commit-config.yaml` are what CI runs.
- TDD, red first. Every step that writes production code is preceded by a test you have watched fail.
- Scope is `check_example_answers` only. Do **not** touch `evaluate_conformance`, `ToolCall`, or `ToolRecorder`. That work is issue #85.
- `expected` stays `float | None`. `_scalar` and `_scalar_value` are not modified.
- Unordered comparison is the default; `ordered: true` is opt-in.
- Never name a tool version outside `.pre-commit-config.yaml`.
- Target version for the release: `0.47.0` in `pyproject.toml:3`, matching a `## [0.47.0]` CHANGELOG heading.

---

### Task 1: Move `_compare` to its own module

`_rows.py` needs the tolerance rule, and `examples.py` will import `_rows`. Leaving `_compare` in `examples.py` makes that a cycle. Pure move, no behaviour change — the suite is the proof.

**Files:**
- Create: `src/agentic_data_contracts/validation/_tolerance.py`
- Modify: `src/agentic_data_contracts/validation/examples.py` (delete `_compare`, import it)
- Modify: `src/agentic_data_contracts/validation/conformance.py:22-30` (import path)

**Interfaces:**
- Consumes: nothing.
- Produces: `_compare(actual: float, expected: float, rel_tol: float, abs_tol: float) -> tuple[float, float, bool]` returning `(abs_diff, rel_diff, matched)`, importable from `agentic_data_contracts.validation._tolerance`.

- [ ] **Step 1: Create the new module with `_compare` moved verbatim**

Cut the whole `_compare` function out of `examples.py`, including its docstring, and paste it into a new file. Keep the name — `conformance.py` carries a comment that names `_compare(actual, expected, ...)` and its argument order as load-bearing.

```python
"""The tolerance rule for comparing a measurement against a certified answer.

Its own module because two comparison modules need it — ``examples`` for a
scalar answer and ``_rows`` for a breakdown — and ``examples`` imports
``_rows``, so leaving it in ``examples`` would make that a cycle.
"""

from __future__ import annotations

import math


def _compare(
    actual: float, expected: float, rel_tol: float, abs_tol: float
) -> tuple[float, float, bool]:
    """<paste the existing docstring verbatim — do not rewrite it>"""
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

- [ ] **Step 2: Update both importers**

In `examples.py`, add to the import block:

```python
from agentic_data_contracts.validation._tolerance import _compare
```

In `conformance.py`, remove `_compare` from the `from ...validation.examples import (...)` block and add:

```python
from agentic_data_contracts.validation._tolerance import _compare
```

- [ ] **Step 3: Run the full suite — a pure move must change nothing**

Run: `uv run pytest -q`
Expected: PASS, same count as before the change (1336).

- [ ] **Step 4: Lint**

Run: `prek run --all-files`
Expected: all hooks pass. If `ruff` reports `math` unused in `examples.py`, check whether anything else there still uses it before removing the import — `_numeric` does (`math.isfinite`), so it must stay.

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/validation/
git commit -m "refactor: _compare moves to its own module

_rows will need the tolerance rule and examples will import _rows, so
leaving _compare in examples would make that a cycle. Pure move; the
suite is unchanged."
```

---

### Task 2: `_rows.py` — key partition and unordered comparison

**Files:**
- Create: `src/agentic_data_contracts/validation/_rows.py`
- Test: `tests/test_validation/test_rows.py`

**Interfaces:**
- Consumes: `_compare` from Task 1.
- Produces:
  - `key_positions(expected_rows: list[list[Any]]) -> tuple[int, ...]`
  - `RowComparison` dataclass with `matched: bool`, `differences: list[str]`, `expected_group_count: int`, `actual_row_count: int`
  - `compare_rows(expected_rows: list[list[Any]], columns: Sequence[str], rows: Sequence[Any], *, ordered: bool, rel_tol: float, abs_tol: float) -> RowComparison`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_validation/test_rows.py`:

```python
"""Tests for comparing a query result against a certified breakdown answer."""

from __future__ import annotations

from decimal import Decimal

import pytest

from agentic_data_contracts.validation._rows import (
    compare_rows,
    key_positions,
)

_EXPECTED = [["EMEA", 5000.0], ["APAC", 3000.0], ["AMER", 2700.0]]
_COLUMNS = ["region", "revenue"]


def _compare(expected, rows, *, columns=None, ordered=False, rel_tol=1e-9, abs_tol=0.0):
    return compare_rows(
        expected,
        columns or _COLUMNS,
        rows,
        ordered=ordered,
        rel_tol=rel_tol,
        abs_tol=abs_tol,
    )


class TestKeyPositions:
    def test_non_numeric_cells_are_the_key(self) -> None:
        assert key_positions(_EXPECTED) == (0,)

    def test_several_dimensions_all_join_the_key(self) -> None:
        rows = [["EMEA", "2025-Q1", 5000.0]]
        assert key_positions(rows) == (0, 1)

    def test_a_row_with_no_measure_is_all_key(self) -> None:
        assert key_positions([["EMEA"], ["APAC"]]) == (0,)


class TestUnorderedComparison:
    def test_same_groups_in_a_different_order_match(self) -> None:
        result = _compare(_EXPECTED, [("AMER", 2700.0), ("EMEA", 5000.0), ("APAC", 3000.0)])
        assert result.matched
        assert result.differences == []
        assert result.actual_row_count == 3

    def test_a_dropped_group_is_named(self) -> None:
        result = _compare(_EXPECTED, [("EMEA", 5000.0), ("AMER", 2700.0)])
        assert not result.matched
        assert any("missing" in d and "APAC" in d for d in result.differences)

    def test_an_extra_group_is_named(self) -> None:
        result = _compare(
            _EXPECTED,
            [("EMEA", 5000.0), ("APAC", 3000.0), ("AMER", 2700.0), ("LATAM", 400.0)],
        )
        assert not result.matched
        assert any("unexpected" in d and "LATAM" in d for d in result.differences)

    def test_the_right_total_with_the_wrong_split_is_a_mismatch(self) -> None:
        # 10700 either way -- the failure this feature exists to catch.
        result = _compare(_EXPECTED, [("EMEA", 5200.0), ("APAC", 2800.0), ("AMER", 2700.0)])
        assert not result.matched
        assert any("EMEA" in d for d in result.differences)
        assert any("APAC" in d for d in result.differences)

    def test_a_value_within_tolerance_matches(self) -> None:
        result = _compare(_EXPECTED, [("EMEA", 5000.4), ("APAC", 3000.0), ("AMER", 2700.0)],
                          abs_tol=0.5)
        assert result.matched

    def test_a_decimal_measure_compares_as_a_number(self) -> None:
        result = _compare(
            _EXPECTED,
            [("EMEA", Decimal("5000.00")), ("APAC", Decimal("3000.00")),
             ("AMER", Decimal("2700.00"))],
        )
        assert result.matched

    def test_a_numeric_key_renders_to_text_before_matching(self) -> None:
        expected = [["2025", 5000.0]]
        result = _compare(expected, [(2025, 5000.0)], columns=["year", "revenue"])
        assert result.matched

    def test_a_key_only_answer_asserts_the_groups_alone(self) -> None:
        result = compare_rows(
            [["EMEA"], ["APAC"]], ["region"], [("APAC",), ("EMEA",)],
            ordered=False, rel_tol=1e-9, abs_tol=0.0,
        )
        assert result.matched

    def test_counts_are_reported(self) -> None:
        result = _compare(_EXPECTED, [("EMEA", 5000.0)])
        assert result.expected_group_count == 3
        assert result.actual_row_count == 1


class TestNullCells:
    def test_null_matches_a_certified_null(self) -> None:
        result = _compare([["EMEA", None]], [("EMEA", None)])
        assert result.matched

    def test_null_where_a_number_was_certified_is_a_mismatch_not_an_error(self) -> None:
        result = _compare([["EMEA", 5000.0]], [("EMEA", None)])
        assert not result.matched
        assert any("EMEA" in d for d in result.differences)
```

- [ ] **Step 2: Run to verify they fail**

Run: `uv run pytest tests/test_validation/test_rows.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'agentic_data_contracts.validation._rows'`

- [ ] **Step 3: Write the module**

Create `src/agentic_data_contracts/validation/_rows.py`:

```python
"""Compare a query result against a certified breakdown answer.

Sibling of ``_scalar``, and for the same reason it gives: one implementation of
the rule, so a second consumer cannot acquire a second chance to get it wrong.

Rows are paired by *key* -- the cells holding something other than a number --
because "compare as a set" and "compare with tolerance" do not compose: set
membership needs hashing and two floats within ``rel_tol`` do not hash together.
The key is read off the certified answer's own cell types rather than declared,
which is the opposite of the call made for ``convention`` and deliberately so: a
convention is not derivable from the schema at any level of model intelligence,
while a row's key is derivable from what the query returned.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from agentic_data_contracts.validation._tolerance import _compare

Key = tuple[str, ...]


@dataclass
class RowComparison:
    """The verdict for one breakdown answer.

    ``differences`` is uncapped and every entry names the group it concerns --
    a missing group and a wrong number are different findings and a caller
    rendering them must be able to say which is which. The counts are carried
    separately so a report can state the whole picture even when it renders
    only the first few differences.
    """

    matched: bool
    differences: list[str]
    expected_group_count: int
    actual_row_count: int


def _is_number(cell: Any) -> bool:
    """``bool`` is excluded deliberately: it is an ``int`` subclass, so a
    ``true`` in a corpus row would otherwise be classed as a measurement and
    compared against ``1.0`` rather than treated as the label it plainly is."""
    return isinstance(cell, (int, float, Decimal)) and not isinstance(cell, bool)


def key_positions(expected_rows: list[list[Any]]) -> tuple[int, ...]:
    """Cell positions that identify a row, read off the first expected row.

    ``VerifiedExample`` validates at load that every expected row agrees on this
    partition, so the first row is representative by then.
    """
    return tuple(i for i, cell in enumerate(expected_rows[0]) if not _is_number(cell))


def _key_of(row: Sequence[Any], positions: tuple[int, ...]) -> Key:
    return tuple(str(row[i]).strip() for i in positions)


def _render(key: Key) -> str:
    return key[0] if len(key) == 1 else "(" + ", ".join(key) + ")"


def _as_number(cell: Any, column: str) -> float:
    try:
        return float(cell)
    except (TypeError, ValueError):
        raise ValueError(
            f"column {column!r} holds a measurement in the certified answer, "
            f"but the query returned {cell!r}"
        ) from None


def _compare_cell(
    where: str,
    column: str,
    expected_cell: Any,
    actual_cell: Any,
    rel_tol: float,
    abs_tol: float,
) -> list[str]:
    if expected_cell is None or actual_cell is None:
        if expected_cell is None and actual_cell is None:
            return []
        return [
            f"{where}: expected {expected_cell!r}, actual {actual_cell!r} "
            f"(column {column})"
        ]
    number = _as_number(actual_cell, column)
    _, rel_diff, matched = _compare(number, float(expected_cell), rel_tol, abs_tol)
    if matched:
        return []
    return [
        f"{where}: expected {expected_cell}, actual {number} "
        f"(rel diff {rel_diff:.3g}, column {column})"
    ]


def compare_rows(
    expected_rows: list[list[Any]],
    columns: Sequence[str],
    rows: Sequence[Any],
    *,
    ordered: bool,
    rel_tol: float,
    abs_tol: float,
) -> RowComparison:
    """Compare a result against a certified breakdown.

    Raises ``ValueError`` for a fault no pairing can resolve -- a column-count
    mismatch, a duplicated group in the result, a non-numeric cell where the
    certified answer holds a measurement. Mirrors ``_scalar``, which raises for
    a result that is not scalar-shaped rather than reporting it as a wrong
    answer: the query did not answer the question that was asked.
    """
    width = len(expected_rows[0])
    if len(columns) != width:
        raise ValueError(
            f"certified answer has {width} column(s), "
            f"query returned {len(columns)}"
        )
    actual = [list(row) for row in rows]
    positions = key_positions(expected_rows)
    differences: list[str] = []

    expected_by_key = {_key_of(row, positions): row for row in expected_rows}
    actual_by_key: dict[Key, list[Any]] = {}
    for row in actual:
        key = _key_of(row, positions)
        if key in actual_by_key:
            raise ValueError(
                f"query returned two rows for group {_render(key)}; "
                "no pairing can resolve that"
            )
        actual_by_key[key] = row

    for key in expected_by_key:
        if key not in actual_by_key:
            differences.append(f"missing group {_render(key)}")
    for key in actual_by_key:
        if key not in expected_by_key:
            differences.append(f"unexpected group {_render(key)}")

    value_positions = [i for i in range(width) if i not in positions]
    for key, expected_row in expected_by_key.items():
        actual_row = actual_by_key.get(key)
        if actual_row is None:
            continue
        for i in value_positions:
            differences.extend(
                _compare_cell(
                    _render(key),
                    columns[i],
                    expected_row[i],
                    actual_row[i],
                    rel_tol,
                    abs_tol,
                )
            )

    return RowComparison(
        matched=not differences,
        differences=differences,
        expected_group_count=len(expected_by_key),
        actual_row_count=len(actual),
    )
```

Note the `ordered` parameter is accepted but not yet honoured — Task 4 adds that branch. Leaving it in the signature now keeps every caller stable.

- [ ] **Step 4: Run to verify they pass**

Run: `uv run pytest tests/test_validation/test_rows.py -q`
Expected: PASS, 13 tests.

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/validation/_rows.py tests/test_validation/test_rows.py
git commit -m "feat: compare a query result against a certified breakdown

Rows pair by key -- the non-numeric cells -- because set membership and
float tolerance do not compose. Missing, unexpected and value-mismatched
groups are named separately: which group differs is the diagnostic the
feature exists to produce."
```

---

### Task 3: `_rows.py` — the three refusals

**Files:**
- Modify: `src/agentic_data_contracts/validation/_rows.py`
- Test: `tests/test_validation/test_rows.py`

**Interfaces:**
- Consumes: `compare_rows` from Task 2.
- Produces: no new names; `compare_rows` raises `ValueError` in three more situations.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_validation/test_rows.py`:

```python
class TestRefusals:
    def test_a_column_count_mismatch_names_both_counts(self) -> None:
        with pytest.raises(ValueError, match="2 column.*returned 3"):
            compare_rows(
                _EXPECTED, ["region", "revenue", "orders"],
                [("EMEA", 5000.0, 12)],
                ordered=False, rel_tol=1e-9, abs_tol=0.0,
            )

    def test_a_duplicated_group_in_the_result_refuses(self) -> None:
        with pytest.raises(ValueError, match="two rows for group EMEA"):
            _compare(_EXPECTED, [("EMEA", 5000.0), ("EMEA", 1.0)])

    def test_a_non_numeric_measure_names_the_column_and_the_value(self) -> None:
        with pytest.raises(ValueError, match="'revenue'.*'N/A'"):
            _compare([["EMEA", 5000.0]], [("EMEA", "N/A")])
```

- [ ] **Step 2: Run to verify they fail**

Run: `uv run pytest tests/test_validation/test_rows.py::TestRefusals -q`
Expected: the first two FAIL on the regex not matching or no exception; the third may already pass, since `_as_number` was written in Task 2. Confirm which fail before implementing — a test that passes immediately is testing existing behaviour, not new.

- [ ] **Step 3: Adjust the messages so all three match**

The column-count and duplicate-group raises already exist from Task 2. If a regex does not match, fix the *message* to name what the test asks for — both counts, and the group — rather than loosening the test. The messages a checker prints are what a corpus author reads at 6pm on a Friday.

- [ ] **Step 4: Run**

Run: `uv run pytest tests/test_validation/test_rows.py -q`
Expected: PASS, 16 tests.

- [ ] **Step 5: Commit**

```bash
git add tests/test_validation/test_rows.py src/agentic_data_contracts/validation/_rows.py
git commit -m "test: pin the three faults a row comparison refuses rather than scores"
```

---

### Task 4: `_rows.py` — ordered mode

**Files:**
- Modify: `src/agentic_data_contracts/validation/_rows.py`
- Test: `tests/test_validation/test_rows.py`

**Interfaces:**
- Consumes: `compare_rows` from Task 2.
- Produces: `compare_rows(..., ordered=True)` pairs by position.

- [ ] **Step 1: Write the failing tests**

```python
class TestOrderedComparison:
    def test_the_declared_order_is_the_answer(self) -> None:
        result = _compare(_EXPECTED, [("EMEA", 5000.0), ("APAC", 3000.0), ("AMER", 2700.0)],
                          ordered=True)
        assert result.matched

    def test_a_reordering_fails_under_ordered(self) -> None:
        result = _compare(_EXPECTED, [("APAC", 3000.0), ("EMEA", 5000.0), ("AMER", 2700.0)],
                          ordered=True)
        assert not result.matched

    def test_a_row_count_difference_is_reported_as_a_count(self) -> None:
        # Position is identity under `ordered`, so "APAC is missing" is a claim
        # the comparison cannot support -- only "a row is missing".
        result = _compare(_EXPECTED, [("EMEA", 5000.0), ("APAC", 3000.0)], ordered=True)
        assert not result.matched
        assert any("expected 3 row" in d for d in result.differences)
        assert not any("missing group" in d for d in result.differences)

    def test_a_repeated_key_is_legitimate_under_ordered(self) -> None:
        expected = [["EMEA", 5000.0], ["EMEA", 3000.0]]
        result = _compare(expected, [("EMEA", 5000.0), ("EMEA", 3000.0)], ordered=True)
        assert result.matched

    def test_a_wrong_number_names_its_row(self) -> None:
        result = _compare(_EXPECTED, [("EMEA", 9999.0), ("APAC", 3000.0), ("AMER", 2700.0)],
                          ordered=True)
        assert any("row 1" in d for d in result.differences)
```

- [ ] **Step 2: Run to verify they fail**

Run: `uv run pytest tests/test_validation/test_rows.py::TestOrderedComparison -q`
Expected: FAIL — `ordered` is accepted but ignored, so the reordering test and the repeated-key test both misbehave (the latter raises "two rows for group EMEA").

- [ ] **Step 3: Add the ordered branch**

In `compare_rows`, immediately after the column-count check and the `actual` coercion:

```python
    if ordered:
        return _compare_ordered(
            expected_rows, columns, actual, rel_tol=rel_tol, abs_tol=abs_tol
        )
```

And add the function:

```python
def _compare_ordered(
    expected_rows: list[list[Any]],
    columns: Sequence[str],
    actual: list[list[Any]],
    *,
    rel_tol: float,
    abs_tol: float,
) -> RowComparison:
    """Pair rows by position, for an answer whose order is the answer.

    A row-count difference is reported as a count rather than as N missing
    groups: position is identity here, so naming *which* group is absent is a
    claim this pairing cannot support.
    """
    differences: list[str] = []
    if len(actual) != len(expected_rows):
        differences.append(
            f"expected {len(expected_rows)} row(s), query returned {len(actual)}"
        )
    for index, (expected_row, actual_row) in enumerate(zip(expected_rows, actual), 1):
        where = f"row {index}"
        for i, expected_cell in enumerate(expected_row):
            actual_cell = actual_row[i]
            if _is_number(expected_cell) or expected_cell is None:
                differences.extend(
                    _compare_cell(
                        where, columns[i], expected_cell, actual_cell, rel_tol, abs_tol
                    )
                )
            elif str(expected_cell).strip() != str(actual_cell).strip():
                differences.append(
                    f"{where}: expected {expected_cell!r}, "
                    f"actual {actual_cell!r} (column {columns[i]})"
                )
    return RowComparison(
        matched=not differences,
        differences=differences,
        expected_group_count=len(expected_rows),
        actual_row_count=len(actual),
    )
```

- [ ] **Step 4: Run the whole file**

Run: `uv run pytest tests/test_validation/test_rows.py -q`
Expected: PASS, 21 tests.

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/validation/_rows.py tests/test_validation/test_rows.py
git commit -m "feat: ordered comparison for an answer whose order is the answer

Pairs by position, and reports a row-count difference as a count: under
ordered, position is identity, so naming which group is absent is a claim
the pairing cannot support."
```

---

### Task 5: `VerifiedExample` — the fields and their load-time rules

**Files:**
- Modify: `src/agentic_data_contracts/validation/examples.py` (`_KNOWN_KEYS`, `VerifiedExample`, `from_dict`)
- Test: `tests/test_validation/test_examples.py`

**Interfaces:**
- Consumes: `key_positions` from Task 2.
- Produces: `VerifiedExample.expected_rows: list[list[Any]] | None`, `VerifiedExample.ordered: bool`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_validation/test_examples.py`:

```python
class TestExpectedRowsLoading:
    def test_a_breakdown_row_loads(self) -> None:
        example = VerifiedExample(
            sql="SELECT region, SUM(amount) FROM analytics.orders GROUP BY region",
            expected_rows=[["EMEA", 5000.0], ["APAC", 3000.0]],
        )
        assert example.expected_rows == [["EMEA", 5000.0], ["APAC", 3000.0]]
        assert example.ordered is False

    def test_from_dict_reads_both_new_keys(self) -> None:
        example = VerifiedExample.from_dict(
            {"sql": "SELECT 1", "expected_rows": [["EMEA", 1.0]], "ordered": True}
        )
        assert example.ordered is True
        assert "expected_rows" not in example.metadata  # not swallowed as metadata

    def test_declaring_both_assertions_raises(self) -> None:
        with pytest.raises(ValueError, match="expected.*expected_rows"):
            VerifiedExample(sql="SELECT 1", expected=1.0, expected_rows=[["EMEA", 1.0]])

    def test_an_empty_list_raises(self) -> None:
        # "Returns nothing" is certifiable as `SELECT COUNT(*) ... expected: 0`,
        # so the empty list carries no capability and stays evidence of a typo.
        with pytest.raises(ValueError, match="non-empty"):
            VerifiedExample(sql="SELECT 1", expected_rows=[])

    def test_ragged_rows_raise(self) -> None:
        with pytest.raises(ValueError, match="same number of cells"):
            VerifiedExample(sql="SELECT 1", expected_rows=[["EMEA", 1.0], ["APAC"]])

    def test_rows_disagreeing_on_the_key_partition_raise(self) -> None:
        with pytest.raises(ValueError, match="same columns"):
            VerifiedExample(sql="SELECT 1", expected_rows=[["EMEA", 1.0], [2.0, 3.0]])

    def test_an_all_numeric_row_raises_and_names_ordered(self) -> None:
        with pytest.raises(ValueError, match="ordered: true"):
            VerifiedExample(sql="SELECT 1", expected_rows=[[2025, 5000.0]])

    def test_an_all_numeric_row_is_fine_when_ordered(self) -> None:
        example = VerifiedExample(
            sql="SELECT 1", expected_rows=[[2025, 5000.0]], ordered=True
        )
        assert example.ordered is True

    def test_duplicate_keys_raise_when_unordered(self) -> None:
        with pytest.raises(ValueError, match="twice"):
            VerifiedExample(sql="SELECT 1", expected_rows=[["EMEA", 1.0], ["EMEA", 2.0]])

    def test_duplicate_keys_are_legitimate_when_ordered(self) -> None:
        # Position is identity under `ordered`; a ranking may name one
        # category twice.
        example = VerifiedExample(
            sql="SELECT 1", expected_rows=[["EMEA", 1.0], ["EMEA", 2.0]], ordered=True
        )
        assert len(example.expected_rows or []) == 2

    def test_ordered_without_an_assertion_is_an_orphan(self) -> None:
        with pytest.raises(ValueError, match="ordered"):
            VerifiedExample(sql="SELECT 1", ordered=True)

    def test_a_tolerance_beside_expected_rows_is_not_an_orphan(self) -> None:
        # The orphan guard keyed on `expected is None`; with a second
        # assertion field that would fire on every valid breakdown row.
        example = VerifiedExample(
            sql="SELECT 1", expected_rows=[["EMEA", 1.0]], abs_tol=0.5
        )
        assert example.abs_tol == 0.5
```

- [ ] **Step 2: Run to verify they fail**

Run: `uv run pytest tests/test_validation/test_examples.py::TestExpectedRowsLoading -q`
Expected: FAIL — `TypeError: VerifiedExample.__init__() got an unexpected keyword argument 'expected_rows'`

- [ ] **Step 3: Add the fields, the validator, and the widened orphan guard**

In `examples.py`, extend `_KNOWN_KEYS` with `"expected_rows"` and `"ordered"`.

Add the validator above `VerifiedExample`:

```python
def _validate_expected_rows(raw: Any, *, ordered: bool) -> list[list[Any]] | None:
    """Validate a certified breakdown from an untrusted corpus row.

    Everything that can be decided from the rows alone is decided here, so a
    malformed corpus fails at load naming the row rather than at execution
    naming a column.
    """
    if raw is None:
        return None
    if not isinstance(raw, list):
        raise ValueError(
            f"'expected_rows' must be a list of rows, got {type(raw).__name__}"
        )
    if not raw:
        raise ValueError(
            "'expected_rows' must be a non-empty list of rows. A query whose "
            "certified answer is no rows is asserted as a count instead: "
            "SELECT COUNT(*) ... with expected: 0."
        )
    rows: list[list[Any]] = []
    for row in raw:
        if not isinstance(row, list) or not row:
            raise ValueError(
                f"each row in 'expected_rows' must be a non-empty list, got {row!r}"
            )
        rows.append(list(row))
    width = len(rows[0])
    if any(len(row) != width for row in rows):
        raise ValueError(
            "every row in 'expected_rows' must have the same number of cells"
        )
    positions = key_positions(rows)
    for row in rows:
        if tuple(i for i, cell in enumerate(row) if not _is_number(cell)) != positions:
            raise ValueError(
                "every row in 'expected_rows' must identify itself by the same "
                "columns — one row's text cells are in different positions"
            )
    if not ordered:
        if not positions:
            raise ValueError(
                "'expected_rows' has no text column to identify a row by, so "
                "rows cannot be matched. Set 'ordered: true' (and ORDER BY in "
                "the SQL), or group by something nameable."
            )
        seen: set[tuple[str, ...]] = set()
        for row in rows:
            key = tuple(str(row[i]).strip() for i in positions)
            if key in seen:
                raise ValueError(
                    f"'expected_rows' names the group {key} twice; an unordered "
                    "answer matches by group, so it cannot state one group twice"
                )
            seen.add(key)
    return rows
```

Import what it needs at the top of `examples.py`:

```python
from agentic_data_contracts.validation._rows import _is_number, key_positions
```

Add the fields to the dataclass, after `expected`:

```python
    expected_rows: list[list[Any]] | None = None
    ordered: bool = False
```

In `__post_init__`, after the existing `self.expected = _numeric(...)` lines and before the orphan block:

```python
        if not isinstance(self.ordered, bool):
            raise ValueError(
                f"'ordered' must be a boolean, got {type(self.ordered).__name__}"
            )
        if self.expected is not None and self.expected_rows is not None:
            raise ValueError(
                "declare 'expected' or 'expected_rows', not both — one asserts a "
                "single number, the other a breakdown"
            )
        self.expected_rows = _validate_expected_rows(
            self.expected_rows, ordered=self.ordered
        )
```

Then change the orphan guard's condition and its list:

```python
        if self.expected is None and self.expected_rows is None:
            orphaned = [
                key
                for key, value in (
                    ("rel_tol", self.rel_tol),
                    ("abs_tol", self.abs_tol),
                    ("time_scoped", self.time_scoped or None),
                    ("ordered", self.ordered or None),
                )
                if value is not None
            ]
```

and widen its message from "Add 'expected'" to "Add 'expected' or 'expected_rows'".

Finally, in `from_dict`, pass both new keys through:

```python
            expected_rows=raw.get("expected_rows"),
            ordered=raw.get("ordered", False),
```

- [ ] **Step 4: Run**

Run: `uv run pytest tests/test_validation/test_examples.py -q`
Expected: PASS. If the pre-existing orphan tests fail, read them before changing anything — they encode the guard's old wording and the message change may be all that is needed.

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/validation/examples.py tests/test_validation/test_examples.py
git commit -m "feat: VerifiedExample accepts a certified breakdown

expected_rows is a sibling of expected rather than a widening of it, so
_scalar's one-column/one-row rule stays enforceable and the two can never
be ambiguous. The orphan guard now asks whether the row asserts anything
at all, rather than whether it declares a scalar."
```

---

### Task 6: The result shape and its rendering

**Files:**
- Modify: `src/agentic_data_contracts/validation/examples.py` (`ExampleAnswerResult`, `ExampleAnswerReport.summary`)
- Test: `tests/test_validation/test_examples.py`

**Interfaces:**
- Consumes: nothing from earlier tasks; fields are populated in Task 7.
- Produces: `ExampleAnswerResult.expected_rows`, `.actual_row_count`, `.row_differences`.

- [ ] **Step 1: Write the failing tests**

```python
class TestBreakdownRendering:
    def _result(self, differences: list[str]) -> ExampleAnswerResult:
        return ExampleAnswerResult(
            example=VerifiedExample(sql="SELECT 1", expected_rows=[["EMEA", 1.0]]),
            status="mismatch",
            expected_rows=[["EMEA", 1.0]],
            actual_row_count=3,
            row_differences=differences,
            label="revenue-by-region",
        )

    def test_a_breakdown_mismatch_names_its_differences(self) -> None:
        report = ExampleAnswerReport(results=[self._result(["missing group APAC"])])
        assert "missing group APAC" in report.summary()
        assert "revenue-by-region" in report.summary()

    def test_only_the_first_three_differences_are_named(self) -> None:
        report = ExampleAnswerReport(
            results=[self._result([f"missing group G{i}" for i in range(6)])]
        )
        summary = report.summary()
        assert "G0" in summary and "G2" in summary
        assert "G3" not in summary
        assert "and 3 more" in summary

    def test_the_empty_report_message_mentions_both_assertion_fields(self) -> None:
        assert "expected_rows" in ExampleAnswerReport(results=[]).summary()
```

- [ ] **Step 2: Run to verify they fail**

Run: `uv run pytest tests/test_validation/test_examples.py::TestBreakdownRendering -q`
Expected: FAIL — `TypeError: ExampleAnswerResult.__init__() got an unexpected keyword argument 'expected_rows'`

- [ ] **Step 3: Add the fields and the rendering branch**

Add to `ExampleAnswerResult`, after `rel_diff`:

```python
    expected_rows: list[list[Any]] | None = None
    actual_row_count: int | None = None
    row_differences: list[str] = field(default_factory=list)
```

Document in its docstring that `abs_diff` / `rel_diff` stay `None` for a breakdown row because there is no single diff, which is why the differences are their own field.

In `summary()`, replace the mismatch branch with one that splits on the assertion kind:

```python
        for r in self.results:
            if r.status == "mismatch" and r.expected_rows is not None:
                shown = "; ".join(r.row_differences[:_MAX_NAMED_DIFFERENCES])
                extra = len(r.row_differences) - _MAX_NAMED_DIFFERENCES
                more = f" (and {extra} more)" if extra > 0 else ""
                lines.append(
                    f"- mismatch `{r.label}`: {len(r.expected_rows)} expected "
                    f"group(s), {r.actual_row_count} row(s) returned, "
                    f"{len(r.row_differences)} difference(s): {shown}{more}"
                )
            elif r.status == "mismatch":
                lines.append(
                    f"- mismatch `{r.label}`: expected {r.expected}, "
                    f"actual {r.actual} (rel diff {_fmt(r.rel_diff)}, "
                    f"rel_tol {_fmt(r.rel_tol)}, abs_tol {_fmt(r.abs_tol)})"
                )
            elif r.status in ("unassertable", "error"):
                lines.append(f"- {r.status} `{r.label}`: {r.reason}")
```

Add the constant beside the other module defaults:

```python
# The counts above stay complete; only the naming is capped, so a reader is
# never misled about how much differed. `row_differences` carries them all.
_MAX_NAMED_DIFFERENCES = 3
```

Widen the empty-report text to name both fields:

```python
                "**Answer checks:** no assertions found — no example declared "
                "an `expected` or `expected_rows` value, so nothing was "
                "checked. Add one to a corpus row, or drop `answers.ok` from "
                "the gate until you do."
```

- [ ] **Step 4: Run**

Run: `uv run pytest tests/test_validation/test_examples.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/validation/examples.py tests/test_validation/test_examples.py
git commit -m "feat: report a breakdown mismatch by naming the groups that differ

Counts are always complete and only the naming is capped at three, so a
fifty-group query cannot flood an MR comment while still telling a reader
how much actually differed."
```

---

### Task 7: Wire it into `check_example_answers`

**Files:**
- Modify: `src/agentic_data_contracts/validation/examples.py` (`check_example_answers` filter, `_check_one`)
- Test: `tests/test_validation/test_examples.py`

**Interfaces:**
- Consumes: `compare_rows` / `RowComparison` (Task 2-4), the fields from Tasks 5 and 6.
- Produces: breakdown rows reaching a verdict instead of being skipped.

- [ ] **Step 1: Extend `SpyAdapter` to serve a full result, and write the failing tests**

`SpyAdapter.execute` currently wraps every canned value in a one-column, one-row `QueryResult`. Let it pass a `QueryResult` through untouched — add this immediately after the `_TWO_COLS` branch:

```python
        if isinstance(value, QueryResult):
            return value
```

Then the tests:

```python
_BREAKDOWN_SQL = (
    "SELECT region, SUM(amount) FROM analytics.orders "
    "WHERE tenant_id = 'acme' GROUP BY region"
)


def _breakdown(rows: list[tuple]) -> QueryResult:
    return QueryResult(columns=["region", "revenue"], rows=rows)


def _asserted_rows(sql: str, expected_rows: list[list], **kw) -> VerifiedExample:
    return VerifiedExample(sql=sql, expected_rows=expected_rows, **kw)


_CERTIFIED = [["EMEA", 5000.0], ["APAC", 3000.0]]


class TestBreakdownAnswerChecks:
    def test_a_correct_breakdown_is_a_match(self, contract: DataContract) -> None:
        adapter = SpyAdapter(
            {_BREAKDOWN_SQL: _breakdown([("APAC", 3000.0), ("EMEA", 5000.0)])}
        )
        report = validate_examples([_asserted_rows(_BREAKDOWN_SQL, _CERTIFIED)], contract)
        answers = check_example_answers(report, adapter=adapter)
        assert [r.status for r in answers.results] == ["match"]
        assert answers.ok

    def test_the_right_total_with_the_wrong_split_is_a_mismatch(
        self, contract: DataContract
    ) -> None:
        adapter = SpyAdapter(
            {_BREAKDOWN_SQL: _breakdown([("EMEA", 5200.0), ("APAC", 2800.0)])}
        )
        report = validate_examples([_asserted_rows(_BREAKDOWN_SQL, _CERTIFIED)], contract)
        answers = check_example_answers(report, adapter=adapter)
        assert [r.status for r in answers.results] == ["mismatch"]
        assert answers.results[0].row_differences

    def test_a_dropped_group_is_a_mismatch(self, contract: DataContract) -> None:
        adapter = SpyAdapter({_BREAKDOWN_SQL: _breakdown([("EMEA", 5000.0)])})
        report = validate_examples([_asserted_rows(_BREAKDOWN_SQL, _CERTIFIED)], contract)
        answers = check_example_answers(report, adapter=adapter)
        assert answers.results[0].status == "mismatch"
        assert any("APAC" in d for d in answers.results[0].row_differences)

    def test_a_structural_fault_is_an_error_not_a_mismatch(
        self, contract: DataContract
    ) -> None:
        adapter = SpyAdapter(
            {_BREAKDOWN_SQL: QueryResult(columns=["region"], rows=[("EMEA",)])}
        )
        report = validate_examples([_asserted_rows(_BREAKDOWN_SQL, _CERTIFIED)], contract)
        answers = check_example_answers(report, adapter=adapter)
        assert answers.results[0].status == "error"

    def test_a_rolling_window_is_unassertable_and_never_executed(
        self, contract: DataContract
    ) -> None:
        sql = (
            "SELECT region, SUM(amount) FROM analytics.orders "
            "WHERE tenant_id = 'acme' AND created_at >= CURRENT_DATE - 30 "
            "GROUP BY region"
        )
        adapter = SpyAdapter({})
        report = validate_examples([_asserted_rows(sql, _CERTIFIED)], contract)
        answers = check_example_answers(report, adapter=adapter)
        assert answers.results[0].status == "unassertable"
        assert adapter.calls == []
```

- [ ] **Step 2: Run to verify they fail**

Run: `uv run pytest tests/test_validation/test_examples.py::TestBreakdownAnswerChecks -q`
Expected: FAIL — every test reports no results at all, because `check_example_answers` still filters on `example.expected is None`.

- [ ] **Step 3: Widen the filter and branch `_check_one`**

In `check_example_answers`, change the skip condition:

```python
        if row.status != "valid" or (
            example.expected is None and example.expected_rows is None
        ):
            continue
```

and its error-path `ExampleAnswerResult(...)`, which passes `expected=example.expected`, to pass `expected_rows=example.expected_rows` as well.

In `_check_one`, replace the opening assertion and `_make` with a form that carries either assertion, and add the breakdown branch after the relative-time check:

```python
    expected = example.expected
    expected_rows = example.expected_rows
    assert expected is not None or expected_rows is not None  # caller's filter

    def _make(status: str, **kw: Any) -> ExampleAnswerResult:
        return ExampleAnswerResult(
            example=example,
            status=status,
            expected=expected,
            expected_rows=expected_rows,
            rel_tol=rel_tol,
            abs_tol=abs_tol,
            label=label,
            **kw,
        )
```

Then, after the existing `unassertable` block and before the `_scalar` call:

```python
    if expected_rows is not None:
        result = adapter.execute(example.sql)
        comparison = compare_rows(
            expected_rows,
            result.columns,
            result.rows,
            ordered=example.ordered,
            rel_tol=rel_tol,
            abs_tol=abs_tol,
        )
        return _make(
            "match" if comparison.matched else "mismatch",
            actual_row_count=comparison.actual_row_count,
            row_differences=comparison.differences,
            reason=None if comparison.matched else "breakdown differs from the certified answer",
        )
```

Import at the top of `examples.py`:

```python
from agentic_data_contracts.validation._rows import compare_rows
```

(merge with the `_is_number, key_positions` import added in Task 5).

The `ValueError`s `compare_rows` raises need no handling here: `check_example_answers` already wraps `_check_one` in the batch guard that turns any exception into an `error` result carrying the message — the same path a non-scalar `_scalar` result takes today.

- [ ] **Step 4: Run the full suite**

Run: `uv run pytest -q`
Expected: PASS.

- [ ] **Step 5: Lint**

Run: `prek run --all-files`
Expected: all hooks pass.

- [ ] **Step 6: Commit**

```bash
git add src/agentic_data_contracts/validation/examples.py tests/test_validation/test_examples.py
git commit -m "feat: check_example_answers verifies a certified breakdown

A breakdown row reached neither pass before: the filter asked for a scalar
`expected`, so the row produced no result and read in the report exactly
like one that was asserted and matched."
```

---

### Task 8: Certify the corpus row that motivated this, and ship it

**Files:**
- Modify: `examples/revenue_agent/verified_examples.yml` (entry #1)
- Modify: `.github/workflows/ci.yml` (the verify-examples step)
- Modify: `README.md` (the verified-examples section)
- Modify: `CHANGELOG.md`, `pyproject.toml:3`

**Interfaces:**
- Consumes: everything above.
- Produces: nothing further.

- [ ] **Step 1: Find the real answer before writing it down**

The corpus is certified against the demo warehouse, so read the number rather than inventing it:

```bash
cd examples/revenue_agent && uv run python -c "
import duckdb
con = duckdb.connect('sample_data.duckdb')
print(con.execute('''
  SELECT c.region, SUM(o.amount) AS revenue
  FROM analytics.orders o JOIN analytics.customers c ON o.customer_id = c.id
  WHERE o.tenant_id = 'acme' AND o.status = 'completed'
  GROUP BY c.region
''').fetchall())"
```

If the database is absent, `uv run python setup_db.py` first. Use exactly the values it prints.

- [ ] **Step 2: Add `expected_rows` to entry #1**

Under `- id: revenue-by-region`, after `sql:`, using the values from Step 1:

```yaml
  expected_rows:
    - [Europe, 7200.00]
    - [North America, 2700.00]
    - [Asia Pacific, 800.00]
```

Update the comment above the entry — it currently says the row passes static checks and the EXPLAIN dry-run, and should now say it also carries a certified breakdown, so the shape the corpus opens with is the shape this feature added.

- [ ] **Step 3: Verify the second pass now asserts it**

Run: `cd examples/revenue_agent && uv run python verify_examples.py`
Expected: `Answer checks:` reports one more match than before, and the `revenue-by-region` row appears with `[MATCH `.

- [ ] **Step 4: Make CI state it rather than tolerate it**

The verify-examples step asserts only that `[MATCH `, `MISMATCH` and `UNASSERTABLE` each appear somewhere, so a breakdown row landing on any of them keeps the step green. Add a line to that step in `.github/workflows/ci.yml`:

```yaml
          # The corpus's first entry is a breakdown, the shape v0.47.0 made
          # assertable. Naming it here means the gate states that it is
          # asserted rather than merely tolerating whatever it does.
          grep -q 'revenue-by-region' out.txt
```

- [ ] **Step 5: Document the field**

In the README's verified-examples section, after the `expected` documentation, add the `expected_rows` YAML example from the spec's "Data model" section, the unordered-by-default rule with the `ordered: true` opt-in, and one sentence that a certified answer of "no rows" is written as `SELECT COUNT(*) ... expected: 0`.

- [ ] **Step 6: Bump the version and write the changelog**

Set `version = "0.47.0"` in `pyproject.toml:3` and add a `## [0.47.0]` section under `### Added` covering: what `expected_rows` asserts; that key identity is inferred from cell types and why; unordered by default with `ordered: true`; the three named difference kinds; the three refusals; and that `evaluate_conformance` still skips breakdowns, pointing at #85.

- [ ] **Step 7: Full verification**

```bash
uv run pytest -q
prek run --all-files
cd examples/revenue_agent && uv run python verify_examples.py
```
Expected: suite green, hooks green, the corpus row asserted.

Then confirm the golden output files are genuinely untouched — no `agent.py` reads the corpus, so this should be a no-op, and a diff here would mean something unexpected is coupled:

```bash
./scripts/regen_examples.sh && git diff --stat examples/
```
Expected: no changes to `examples/*/expected_output.txt`.

- [ ] **Step 8: Commit**

```bash
git add examples/ .github/ README.md CHANGELOG.md pyproject.toml
git commit -m "feat: certify the corpus's first entry (v0.47.0)

revenue-by-region is a GROUP BY, and it opened the corpus while being the
one shape the corpus could not certify. It now carries a real
expected_rows, so the file that demonstrated the gap demonstrates the fix."
```

---

## Self-Review

**Spec coverage.** Every section maps to a task: data model → 5; comparison module, key partition, cell rules, three difference kinds → 2; refusals → 3; ordered mode → 4; load-time validation and the widened orphan guard → 5; result and reporting → 6; `check_example_answers` integration → 7; corpus, CI and docs → 8. The `_compare` move (Task 1) is not in the spec — it is an implementation consequence of the spec's "one tolerance rule, one place to set it" meeting Python's import rules, and it is called out as such.

**Placeholder scan.** No TBD/TODO. Every code step carries the code. Task 3's step 3 deliberately says "fix the message, not the test" rather than showing final strings, because the strings depend on what Task 2's author wrote — the step names the criterion instead, which is the real requirement.

**Type consistency.** `compare_rows` keeps one signature across Tasks 2, 4 and 7. `RowComparison`'s four fields are named identically wherever they are read. `key_positions` and `_is_number` are defined in Task 2 and imported by name in Task 5. `ExampleAnswerResult`'s three new fields are named identically in Tasks 6 and 7. `_MAX_NAMED_DIFFERENCES` is defined where it is first used.

**One thing an executor should not silently resolve.** Task 8 Step 1 reads the certified numbers out of the demo warehouse. If those values disagree with the region names assumed in Step 2's snippet, the warehouse wins — the point of a certified answer is that a human verified what the query returns, and inventing plausible values would make the corpus's own showcase row a fiction.
