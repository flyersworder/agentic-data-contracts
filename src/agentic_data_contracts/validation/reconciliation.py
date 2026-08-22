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

from agentic_data_contracts.validation._scalar import _scalar

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

    The default ``rel_tol`` assumes the operands are **exact**. An operand
    declared at limited precision (a rate carried at three decimals, say) makes
    the identity approximate by construction, and its residual — ~0.03% for that
    rate — is reported identically to real drift. Widen ``rel_tol`` to the
    precision the operands actually have rather than reading it as a finding.
    Units are the author's responsibility too: a percentage-scaled operand makes
    a ``product`` identity false by ~100x, which ``implied_parent`` against
    ``actual_parent`` names on sight.

    The relative term here is anchored on the measured parent (``actual_parent``),
    unlike ``check_example_answers``'s ``_compare``, which anchors on ``expected``.
    That is deliberate in both places: this function compares two *measurements*
    with no privileged side, while ``check_example_answers`` has a certified
    reference to anchor against.
    """
    # Reuse the operator vocabulary from the semantic layer as the single source
    # of truth (so a change there can't silently disagree here). Imported inside
    # the function rather than at module top to avoid the same circular import
    # documented on the TYPE_CHECKING block above; by call time every module is
    # fully initialized, so this deferred import is safe.
    from agentic_data_contracts.semantic.base import (
        _BINARY_OPERATORS,
        VALID_OPERATORS,
    )

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

    # Validate the operator and arity up front, before running any query, so a
    # malformed decomposition fails fast instead of after N wasted round-trips.
    if decomp.operator not in VALID_OPERATORS:
        raise ValueError(
            f"unknown decomposition operator {decomp.operator!r}; "
            f"expected one of {sorted(VALID_OPERATORS)}"
        )
    if decomp.operator in _BINARY_OPERATORS and len(declared) != 2:
        raise ValueError(
            f"operator {decomp.operator!r} requires exactly 2 operands, "
            f"got {len(declared)}"
        )
    if decomp.operator not in _BINARY_OPERATORS and len(declared) < 2:
        raise ValueError(
            f"operator {decomp.operator!r} requires at least 2 operands, "
            f"got {len(declared)}"
        )

    def _make(
        *,
        operands: dict[str, float],
        implied_parent: float,
        actual_parent: float,
        abs_diff: float,
        rel_diff: float,
        reconciles: bool,
        reason: str | None,
    ) -> ReconciliationResult:
        # Fills the four fields common to every branch (metric, operator, and
        # the two tolerances) so each call site varies only the measurements.
        return ReconciliationResult(
            metric=metric.name,
            operator=decomp.operator,
            operands=operands,
            implied_parent=implied_parent,
            actual_parent=actual_parent,
            abs_diff=abs_diff,
            rel_diff=rel_diff,
            reconciles=reconciles,
            rel_tol=rel_tol,
            abs_tol=abs_tol,
            reason=reason,
        )

    measured: dict[str, float] = {}
    missing_reasons: list[str] = []
    for name in declared:
        value, reason = _scalar(adapter, operand_sql[name], f"operand {name!r}")
        if value is None:
            assert reason is not None  # _scalar pairs a None value with a reason
            missing_reasons.append(reason)
        else:
            measured[name] = value
    parent_value, parent_reason = _scalar(adapter, parent_sql, "parent")

    if missing_reasons or parent_value is None:
        reasons = list(missing_reasons)
        if parent_reason is not None:
            reasons.append(parent_reason)
        return _make(
            operands=measured,
            implied_parent=math.nan,
            actual_parent=math.nan if parent_value is None else parent_value,
            abs_diff=math.nan,
            rel_diff=math.nan,
            reconciles=False,
            reason="; ".join(reasons),
        )

    actual_parent = parent_value
    values = [measured[name] for name in declared]

    if decomp.operator == "ratio" and values[1] == 0:
        return _make(
            operands=dict(zip(declared, values, strict=True)),
            implied_parent=math.inf,
            actual_parent=actual_parent,
            abs_diff=math.inf,
            rel_diff=math.inf,
            reconciles=False,
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

    return _make(
        operands=dict(zip(declared, values, strict=True)),
        implied_parent=implied,
        actual_parent=actual_parent,
        abs_diff=abs_diff,
        rel_diff=rel_diff,
        reconciles=reconciles,
        reason=None if reconciles else "identity does not hold within tolerance",
    )
