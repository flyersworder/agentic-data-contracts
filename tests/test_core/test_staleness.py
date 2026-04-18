"""Tests for the stale-review detector."""

from __future__ import annotations

from datetime import date

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.schema import (
    AllowedTable,
    DataContractSchema,
    Domain,
    SemanticConfig,
)
from agentic_data_contracts.core.staleness import StaleFinding, find_stale_reviews
from agentic_data_contracts.semantic.base import MetricImpact


def _contract(*domains: Domain) -> DataContract:
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate(
                    {"schema": "analytics", "tables": ["orders"]}
                )
            ],
            domains=list(domains),
        ),
    )
    return DataContract(schema)


class TestDomainStaleness:
    def test_domain_without_last_reviewed_is_stale(self) -> None:
        """Missing timestamp = never reviewed = stale."""
        contract = _contract(
            Domain(name="revenue", summary="", description="x"),
        )
        findings = find_stale_reviews(contract, impacts=[], today=date(2026, 4, 18))
        assert len(findings) == 1
        f = findings[0]
        assert isinstance(f, StaleFinding)
        assert f.kind == "domain"
        assert f.name == "revenue"
        assert f.last_reviewed is None
        assert f.age_days is None

    def test_domain_reviewed_within_threshold_is_fresh(self) -> None:
        contract = _contract(
            Domain(
                name="revenue",
                summary="",
                description="x",
                last_reviewed=date(2026, 3, 1),
            ),
        )
        findings = find_stale_reviews(
            contract, impacts=[], today=date(2026, 4, 18), threshold_days=90
        )
        assert findings == []

    def test_domain_reviewed_beyond_threshold_is_stale(self) -> None:
        contract = _contract(
            Domain(
                name="revenue",
                summary="",
                description="x",
                last_reviewed=date(2025, 11, 1),
            ),
        )
        findings = find_stale_reviews(
            contract, impacts=[], today=date(2026, 4, 18), threshold_days=90
        )
        assert len(findings) == 1
        f = findings[0]
        assert f.kind == "domain"
        assert f.name == "revenue"
        assert f.last_reviewed == date(2025, 11, 1)
        assert f.age_days == (date(2026, 4, 18) - date(2025, 11, 1)).days
        assert f.threshold_days == 90

    def test_exactly_at_threshold_is_fresh(self) -> None:
        """Boundary case — age == threshold is still fresh (inclusive)."""
        contract = _contract(
            Domain(
                name="revenue",
                summary="",
                description="x",
                last_reviewed=date(2026, 1, 18),  # exactly 90 days before 2026-04-18
            ),
        )
        findings = find_stale_reviews(
            contract, impacts=[], today=date(2026, 4, 18), threshold_days=90
        )
        assert findings == []

    def test_custom_threshold_respected(self) -> None:
        contract = _contract(
            Domain(
                name="revenue",
                summary="",
                description="x",
                last_reviewed=date(2026, 3, 1),  # 48 days old
            ),
        )
        fresh = find_stale_reviews(
            contract, impacts=[], today=date(2026, 4, 18), threshold_days=90
        )
        stale = find_stale_reviews(
            contract, impacts=[], today=date(2026, 4, 18), threshold_days=30
        )
        assert fresh == []
        assert len(stale) == 1


class TestMetricImpactStaleness:
    def test_impact_without_last_reviewed_is_stale(self) -> None:
        contract = _contract()
        impacts = [MetricImpact(from_metric="a", to_metric="b")]
        findings = find_stale_reviews(
            contract, impacts=impacts, today=date(2026, 4, 18)
        )
        assert len(findings) == 1
        assert findings[0].kind == "metric_impact"
        assert findings[0].name == "a -> b"

    def test_impact_fresh_is_not_flagged(self) -> None:
        contract = _contract()
        impacts = [
            MetricImpact(from_metric="a", to_metric="b", last_reviewed=date(2026, 3, 1))
        ]
        findings = find_stale_reviews(
            contract, impacts=impacts, today=date(2026, 4, 18), threshold_days=90
        )
        assert findings == []

    def test_impact_context_carries_confidence_and_endpoints(self) -> None:
        """Context carries metadata callers need to filter (e.g. only fail on
        `verified` edges) or to look up the originating impact."""
        contract = _contract()
        impacts = [
            MetricImpact(
                from_metric="a",
                to_metric="b",
                confidence="verified",
                direction="positive",
                last_reviewed=date(2025, 1, 1),
            )
        ]
        findings = find_stale_reviews(
            contract, impacts=impacts, today=date(2026, 4, 18)
        )
        assert len(findings) == 1
        ctx = findings[0].context
        assert ctx["confidence"] == "verified"
        assert ctx["direction"] == "positive"
        assert ctx["from_metric"] == "a"
        assert ctx["to_metric"] == "b"

    def test_domain_context_is_empty(self) -> None:
        """Domain findings have no kind-specific metadata today."""
        contract = _contract(Domain(name="revenue", summary="", description="x"))
        findings = find_stale_reviews(contract, impacts=[], today=date(2026, 4, 18))
        assert findings[0].context == {}


class TestMixedAndOrdering:
    def test_domain_and_impact_both_flagged(self) -> None:
        contract = _contract(
            Domain(name="revenue", summary="", description="x"),  # missing
        )
        impacts = [
            MetricImpact(from_metric="a", to_metric="b", last_reviewed=date(2024, 1, 1))
        ]
        findings = find_stale_reviews(
            contract, impacts=impacts, today=date(2026, 4, 18)
        )
        assert len(findings) == 2
        kinds = {f.kind for f in findings}
        assert kinds == {"domain", "metric_impact"}

    def test_today_defaults_to_date_today(self) -> None:
        """Callers may omit `today`; detector uses the current date."""
        contract = _contract(
            Domain(
                name="revenue",
                summary="",
                description="x",
                last_reviewed=date(1970, 1, 1),  # absurdly old
            ),
        )
        findings = find_stale_reviews(contract, impacts=[])
        assert len(findings) == 1
        assert findings[0].kind == "domain"

    def test_multiple_domains_mixed_fresh_and_stale(self) -> None:
        """Only stale domains appear; ordering mirrors declaration order."""
        contract = _contract(
            Domain(name="a", summary="", description="x"),  # missing → stale
            Domain(
                name="b",
                summary="",
                description="x",
                last_reviewed=date(2026, 3, 1),  # fresh
            ),
            Domain(
                name="c",
                summary="",
                description="x",
                last_reviewed=date(2025, 1, 1),  # stale
            ),
        )
        findings = find_stale_reviews(
            contract, impacts=[], today=date(2026, 4, 18), threshold_days=90
        )
        assert [f.name for f in findings] == ["a", "c"]

    def test_threshold_zero_requires_review_today(self) -> None:
        """`threshold_days=0` means 'stale unless reviewed on exactly `today`'."""
        contract = _contract(
            Domain(
                name="fresh",
                summary="",
                description="x",
                last_reviewed=date(2026, 4, 18),
            ),
            Domain(
                name="yesterday",
                summary="",
                description="x",
                last_reviewed=date(2026, 4, 17),
            ),
        )
        findings = find_stale_reviews(
            contract, impacts=[], today=date(2026, 4, 18), threshold_days=0
        )
        assert [f.name for f in findings] == ["yesterday"]

    def test_future_last_reviewed_treated_as_fresh(self) -> None:
        """Negative age (review date in the future) is not stale."""
        contract = _contract(
            Domain(
                name="future",
                summary="",
                description="x",
                last_reviewed=date(2027, 1, 1),
            ),
        )
        findings = find_stale_reviews(contract, impacts=[], today=date(2026, 4, 18))
        assert findings == []
