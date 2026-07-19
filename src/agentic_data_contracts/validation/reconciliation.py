"""Reconcile a metric's declared arithmetic decomposition against live data.

Executes the parent metric and each declared operand (via caller-supplied
scalar SQL), applies the decomposition operator, and checks the identity holds
within tolerance. This is a contract-integrity check: it catches an identity
that has become false in the data (ETL drift, definition drift) that the
per-query validators never see. It reports the discrepancy and the numbers; it
does not infer the cause (that is agent-owned diagnosis).
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # Deferred to avoid a circular import: adapters.base imports
    # validation.explain, which initializes this package (validation/__init__)
    # before adapters.base finishes defining DatabaseAdapter/TableSchema, and
    # semantic.base itself imports TableSchema from adapters.base at module
    # level. Safe at runtime because `from __future__ import annotations`
    # keeps annotations unevaluated.
    from agentic_data_contracts.adapters.base import DatabaseAdapter
    from agentic_data_contracts.semantic.base import MetricDefinition


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
