"""Attribute a metric's change to its factors under the contract's convention.

Pure arithmetic: measured values in, contribution breakdown out. No adapter, no
SQL, and deliberately no time-window semantics — the caller owns *when* each
value was measured (calendar vs. cohort windows are analytics-domain logic, not
governance), and the contract owns where the cross term goes.

The cross-term placement is a non-inferable business fact. An agent told only
the factors picks a placement silently and presents it as canonical; two such
reports are indistinguishable because both sum correctly. That is what
``Decomposition.convention`` declares and what this module applies.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # Deferred for the circular-import reason documented in
    # ``validation.reconciliation``: ``semantic.base`` imports ``TableSchema``
    # from ``adapters.base``, which initializes this package first. Safe at
    # runtime because ``from __future__ import annotations`` keeps annotations
    # unevaluated.
    from agentic_data_contracts.semantic.base import Decomposition, MetricDefinition

#: Key under which an ``explicit`` breakdown reports the unattributed residual.
#: A constant so callers need not hardcode the string.
INTERACTION_KEY = "interaction"


@dataclass(frozen=True)
class AttributionResult:
    metric: str
    operator: str
    convention: str | None
    convention_operand: str | None
    delta_parent: float
    contributions: dict[str, float]
    interaction: float
    shares: dict[str, float] | None
    # Populated only by ``check_attribution``.
    reported: dict[str, float] | None = None
    deviations: dict[str, float] | None = None
    matches: bool | None = None
    sums_to_delta: bool | None = None
    rel_tol: float | None = None
    abs_tol: float | None = None
    reason: str | None = None


def _resolve(metric: MetricDefinition, index: int) -> Decomposition:
    """Select the declared decomposition, raising before any arithmetic."""
    if not metric.decompositions:
        raise ValueError(f"metric {metric.name!r} declares no decompositions")
    if not 0 <= index < len(metric.decompositions):
        raise ValueError(
            f"decomposition index {index} out of range for metric "
            f"{metric.name!r} ({len(metric.decompositions)} declared)"
        )
    return metric.decompositions[index]


def _check_keys(
    values: Mapping[str, float], label: str, operands: list[str], metric_name: str
) -> None:
    if set(values) != set(operands):
        raise ValueError(
            f"{label} keys {sorted(values)} do not match the declared operands "
            f"{operands} of metric {metric_name!r}"
        )


def _main_effects(
    operator: str,
    operands: list[str],
    before: Mapping[str, float],
    after: Mapping[str, float],
) -> dict[str, float]:
    """Each operand's effect with every other operand held at its ``before``.

    The residual left over is the interaction, computed by the caller as
    ``delta - sum(effects)`` so it is a single lumped term at any arity rather
    than the 2**n - 1 expansion. That matches what an analyst reports as "the
    cross term" and is what the #67 explicit-cross runs actually produced.
    """
    if operator == "product":
        effects: dict[str, float] = {}
        for i, name in enumerate(operands):
            others = 1.0
            for j, other in enumerate(operands):
                if j != i:
                    others *= before[other]
            effects[name] = (after[name] - before[name]) * others
        return effects
    if operator == "ratio":
        num, den = operands
        return {
            num: (after[num] - before[num]) / before[den],
            den: before[num] / after[den] - before[num] / before[den],
        }
    if operator == "sum":
        return {name: after[name] - before[name] for name in operands}
    first, second = operands
    return {
        first: after[first] - before[first],
        second: -(after[second] - before[second]),
    }


def _place(
    convention: str | None,
    convention_operand: str | None,
    effects: dict[str, float],
    interaction: float,
) -> dict[str, float]:
    """Distribute the interaction residual as the declared convention says."""
    contributions = dict(effects)
    if convention == "split_evenly":
        share = interaction / len(contributions)
        for name in contributions:
            contributions[name] += share
    elif convention == "fold_into":
        # Unreachable in practice: attribute_change validates convention and
        # convention_operand before calling here (and validate_decompositions
        # does the same at load time), so this documents the invariant this
        # function relies on rather than guarding against a real call path.
        assert convention_operand is not None
        contributions[convention_operand] += interaction
    # "explicit" (and the linear operators) leave the residual where it is.
    return contributions


def attribute_change(
    metric: MetricDefinition,
    *,
    before: Mapping[str, float],
    after: Mapping[str, float],
    decomposition: int = 0,
) -> AttributionResult:
    """Break a metric's change into per-factor contributions.

    ``before`` / ``after`` map each declared operand to its measured value at
    the two points being compared. The decomposition's declared ``convention``
    places the interaction residual; a ``product`` or ``ratio`` decomposition
    that declares none raises, because the answer would otherwise be one of
    several defensible numbers with no way to tell which was used.
    """
    from agentic_data_contracts.semantic.base import (
        _CROSS_TERM_OPERATORS,
        VALID_CONVENTIONS,
    )
    from agentic_data_contracts.validation.reconciliation import _apply_operator

    decomp = _resolve(metric, decomposition)
    operands = list(decomp.operands)
    _check_keys(before, "before", operands, metric.name)
    _check_keys(after, "after", operands, metric.name)

    if decomp.operator in _CROSS_TERM_OPERATORS and decomp.convention is None:
        raise ValueError(
            f"metric {metric.name!r} decomposition declares no attribution "
            f"convention; a {decomp.operator!r} identity has a cross term whose "
            f"placement changes the answer, so it must be declared"
        )
    # attribute_change accepts an arbitrary MetricDefinition and cannot assume
    # it came through validate_decompositions at load time, so re-validate the
    # convention here rather than let a malformed one reach _place -- which
    # would otherwise assert (or, under python -O, KeyError on contributions
    # [None]) partway through the arithmetic instead of raising cleanly.
    if decomp.convention is not None and decomp.convention not in VALID_CONVENTIONS:
        raise ValueError(
            f"metric {metric.name!r} decomposition has unknown attribution "
            f"convention {decomp.convention!r}; expected one of "
            f"{sorted(VALID_CONVENTIONS)}"
        )
    if decomp.convention == "fold_into" and (
        decomp.convention_operand is None or decomp.convention_operand not in operands
    ):
        raise ValueError(
            f"metric {metric.name!r} decomposition convention 'fold_into' "
            f"requires 'convention_operand' naming one of its operands "
            f"{operands}, got {decomp.convention_operand!r}"
        )
    if decomp.operator == "ratio":
        for label, values in (("before", before), ("after", after)):
            if values[operands[1]] == 0:
                raise ValueError(
                    f"ratio denominator (operand {operands[1]!r}) is zero at {label}"
                )

    delta = _apply_operator(decomp.operator, [after[n] for n in operands]) - (
        _apply_operator(decomp.operator, [before[n] for n in operands])
    )
    effects = _main_effects(decomp.operator, operands, before, after)
    interaction = delta - math.fsum(effects.values())
    contributions = _place(
        decomp.convention, decomp.convention_operand, effects, interaction
    )
    shares = (
        None
        if delta == 0
        else {name: value / delta for name, value in contributions.items()}
    )
    return AttributionResult(
        metric=metric.name,
        operator=decomp.operator,
        convention=decomp.convention,
        convention_operand=decomp.convention_operand,
        delta_parent=delta,
        contributions=contributions,
        interaction=interaction,
        shares=shares,
    )


def check_attribution(
    metric: MetricDefinition,
    *,
    before: Mapping[str, float],
    after: Mapping[str, float],
    reported: Mapping[str, float],
    rel_tol: float = 1e-4,
    abs_tol: float = 0.0,
    decomposition: int = 0,
) -> AttributionResult:
    """Check a reported breakdown against the contract's declared convention.

    The intended caller is an **eval harness**, not CI and not production: a
    ``reported`` breakdown exists only inside an agent's written answer, and
    post-hoc checking is the wrong shape for a failure with an agent in the
    loop by construction. What it is good for is measuring whether declaring a
    convention changes what an agent reports.

    Under ``explicit``, *reported* must also carry the residual under
    ``INTERACTION_KEY``; under the other conventions that key is rejected,
    because the residual has already been distributed and reporting it again
    double-counts. Every deviation is judged against
    ``max(abs_tol, rel_tol * abs(delta_parent))`` -- a contribution is
    meaningful as a share of the total change, and a per-contribution relative
    tolerance explodes when a contribution is near zero.
    """
    expected = attribute_change(
        metric, before=before, after=after, decomposition=decomposition
    )
    target = dict(expected.contributions)
    if expected.convention == "explicit":
        target[INTERACTION_KEY] = expected.interaction

    if set(reported) != set(target):
        raise ValueError(
            f"reported keys {sorted(reported)} do not match the expected "
            f"breakdown {sorted(target)} for metric {metric.name!r} under "
            f"convention {expected.convention!r}"
        )

    tolerance = max(abs_tol, rel_tol * abs(expected.delta_parent))
    deviations = {name: reported[name] - value for name, value in target.items()}
    matches = all(abs(d) <= tolerance for d in deviations.values())
    sums_to_delta = (
        abs(math.fsum(reported.values()) - expected.delta_parent) <= tolerance
    )
    return replace(
        expected,
        reported=dict(reported),
        deviations=deviations,
        matches=matches,
        sums_to_delta=sums_to_delta,
        rel_tol=rel_tol,
        abs_tol=abs_tol,
        reason=(
            None
            if matches
            else "reported contributions do not match the declared convention "
            "within tolerance"
        ),
    )
