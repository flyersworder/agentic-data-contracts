"""Stale-review detector for governance artefacts in a data contract.

Flags domains and metric-impact edges whose ``last_reviewed`` timestamp is
missing or older than ``threshold_days``. Missing timestamp is treated as
stale — otherwise a contract that never adopts the field sidesteps the
check entirely, which defeats the point.

The detector is pure and deterministic: pass ``today`` explicitly in tests;
in production callers can omit it and let it default to ``date.today()``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from agentic_data_contracts.core.contract import DataContract
    from agentic_data_contracts.semantic.base import MetricImpact


@dataclass(frozen=True)
class StaleFinding:
    """A single governance artefact that needs human re-review.

    ``context`` carries kind-specific metadata that callers may use to filter
    or format findings (e.g. ``{"confidence": "verified"}`` for
    ``metric_impact`` entries). Keeping this open-ended avoids per-kind
    fields on a shared value object.
    """

    kind: str  # "domain" | "metric_impact"
    name: str
    last_reviewed: date | None
    age_days: int | None  # None when last_reviewed is None
    threshold_days: int
    context: dict[str, Any] = field(default_factory=dict)


def find_stale_reviews(
    contract: DataContract,
    impacts: list[MetricImpact],
    *,
    threshold_days: int = 90,
    today: date | None = None,
) -> list[StaleFinding]:
    """Return every domain / metric-impact whose review is missing or expired.

    A finding is produced when ``last_reviewed`` is ``None`` (reported with
    ``age_days=None``) or when ``(today - last_reviewed).days > threshold_days``.
    Equality with the threshold is treated as fresh (inclusive boundary).

    Contracts that have never adopted ``last_reviewed`` will report every
    domain and impact as stale on first run. To grandfather in existing
    artefacts during rollout, either add ``last_reviewed: <today>`` to each
    entry or filter the result by ``f.age_days is not None``.
    """
    as_of = today if today is not None else date.today()
    findings: list[StaleFinding] = []

    for domain in contract.schema.semantic.domains:
        finding = _evaluate(
            kind="domain",
            name=domain.name,
            last_reviewed=domain.last_reviewed,
            as_of=as_of,
            threshold_days=threshold_days,
        )
        if finding is not None:
            findings.append(finding)

    for impact in impacts:
        finding = _evaluate(
            kind="metric_impact",
            name=f"{impact.from_metric} -> {impact.to_metric}",
            last_reviewed=impact.last_reviewed,
            as_of=as_of,
            threshold_days=threshold_days,
            context={
                "from_metric": impact.from_metric,
                "to_metric": impact.to_metric,
                "confidence": impact.confidence,
                "direction": impact.direction,
            },
        )
        if finding is not None:
            findings.append(finding)

    return findings


def _evaluate(
    *,
    kind: str,
    name: str,
    last_reviewed: date | None,
    as_of: date,
    threshold_days: int,
    context: dict[str, Any] | None = None,
) -> StaleFinding | None:
    ctx = context if context is not None else {}
    if last_reviewed is None:
        return StaleFinding(
            kind=kind,
            name=name,
            last_reviewed=None,
            age_days=None,
            threshold_days=threshold_days,
            context=ctx,
        )
    age_days = (as_of - last_reviewed).days
    if age_days <= threshold_days:
        return None
    return StaleFinding(
        kind=kind,
        name=name,
        last_reviewed=last_reviewed,
        age_days=age_days,
        threshold_days=threshold_days,
        context=ctx,
    )
