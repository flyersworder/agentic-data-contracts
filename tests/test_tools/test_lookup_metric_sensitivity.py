"""lookup_metric surfaces declared sensitivity properties."""

from __future__ import annotations

from datetime import date

from agentic_data_contracts.semantic.base import (
    MetricDefinition,
    SensitivityProperty,
    Shadow,
)
from agentic_data_contracts.tools.factory import _metric_details

PROP = SensitivityProperty(
    name="attribution_is_first_touch",
    description="A touchpoint after qualification cannot change a first touch.",
    shadow=Shadow(table="mkt.touchpoints", sql="SELECT * FROM mkt.touchpoints"),
    expect="unchanged",
)


def _details(metric: MetricDefinition) -> dict:
    # `impact_index` is positional; `today` and `threshold_days` are required
    # keywords feeding the freshness fields, which this test does not exercise.
    return _metric_details(metric, {}, today=date(2026, 9, 18), threshold_days=180)


class TestLookupMetric:
    def test_surfaces_declared_properties(self) -> None:
        metric = MetricDefinition(
            name="mql_count",
            description="",
            sql_expression="COUNT(*)",
            sensitivity=[PROP],
        )
        data = _details(metric)
        assert data["sensitivity"] == [
            {
                "name": "attribution_is_first_touch",
                "description": (
                    "A touchpoint after qualification cannot change a first touch."
                ),
                "expect": "unchanged",
            }
        ]

    def test_does_not_leak_the_shadow_sql(self) -> None:
        metric = MetricDefinition(
            name="mql_count",
            description="",
            sql_expression="COUNT(*)",
            sensitivity=[PROP],
        )
        assert "SELECT" not in repr(_details(metric)["sensitivity"])

    def test_omits_the_key_when_none_declared(self) -> None:
        metric = MetricDefinition(name="m", description="", sql_expression="COUNT(*)")
        assert "sensitivity" not in _details(metric)
