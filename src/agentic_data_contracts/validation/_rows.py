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
        # Re-derives the key-vs-value classification per cell rather than
        # calling `key_positions` once, as the unordered path below does. Do
        # NOT factor this out to share with `key_positions`: the two paths
        # classify `None` differently on purpose. Unordered treats a `None`
        # cell as a KEY (`_is_number(None)` is False, so `key_positions` puts
        # its position in `positions`); here a `None` cell is routed to
        # `_compare_cell` as a VALUE (`or expected_cell is None` below).
        # Sharing one derivation would force one rule onto both and silently
        # break whichever path didn't get it.
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
            f"certified answer has {width} column(s), query returned {len(columns)}"
        )
    actual = [list(row) for row in rows]
    if ordered:
        return _compare_ordered(
            expected_rows, columns, actual, rel_tol=rel_tol, abs_tol=abs_tol
        )
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
