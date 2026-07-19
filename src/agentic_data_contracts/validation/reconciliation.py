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
