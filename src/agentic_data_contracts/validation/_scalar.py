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
