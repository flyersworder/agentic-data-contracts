"""Tests for metric-first domain membership reverse-lookup."""

from __future__ import annotations

from agentic_data_contracts.semantic.base import (
    MetricDefinition,
    domain_metric_counts,
    metrics_in_domain,
)


def _metric(name: str, domains: list[str]) -> MetricDefinition:
    return MetricDefinition(
        name=name,
        description=f"{name} description",
        sql_expression="SUM(x)",
        domains=domains,
    )


def test_metrics_in_domain_returns_self_declared_members() -> None:
    metrics = [
        _metric("total_revenue", ["revenue"]),
        _metric("active_customers", ["engagement", "revenue"]),
        _metric("page_views", ["engagement"]),
    ]
    names = [m.name for m in metrics_in_domain(metrics, "revenue")]
    assert names == ["total_revenue", "active_customers"]


def test_metrics_in_domain_unknown_domain_is_empty() -> None:
    metrics = [_metric("total_revenue", ["revenue"])]
    assert metrics_in_domain(metrics, "nonexistent") == []


def test_metrics_in_domain_empty_metrics_is_empty() -> None:
    assert metrics_in_domain([], "revenue") == []


def test_domain_metric_counts_matches_membership() -> None:
    metrics = [
        _metric("total_revenue", ["revenue"]),
        _metric("active_customers", ["engagement", "revenue"]),
    ]
    counts = domain_metric_counts(metrics)
    assert counts["revenue"] == len(metrics_in_domain(metrics, "revenue"))  # == 2
    assert counts["engagement"] == 1
    assert counts["nonexistent"] == 0  # Counter default, no KeyError


def test_domain_metric_counts_dedupes_within_a_metric() -> None:
    """A metric listing the same domain twice counts once — the displayed count
    must never exceed the actual member count from metrics_in_domain."""
    metrics = [_metric("total_revenue", ["revenue", "revenue"])]
    counts = domain_metric_counts(metrics)
    assert counts["revenue"] == 1 == len(metrics_in_domain(metrics, "revenue"))
