"""Contract-declared sensitivity properties: schema, parsing, validation."""

from __future__ import annotations

import pytest

from agentic_data_contracts.semantic.base import (
    MetricDefinition,
    SensitivityProperty,
    Shadow,
)
from agentic_data_contracts.semantic.yaml_source import YamlSource

SHADOW_SQL = (
    "SELECT * FROM mkt.touchpoints UNION ALL "
    "SELECT lead_id, 'display', qualified_date + 1 FROM mkt.lead_scores"
)


def _raw(**overrides: object) -> dict:
    """A one-metric semantic document carrying one sensitivity property."""
    prop: dict = {
        "name": "attribution_is_first_touch",
        "description": (
            "A touchpoint after qualification cannot change a first-touch attribution."
        ),
        "shadow": {"table": "mkt.touchpoints", "sql": SHADOW_SQL},
        "expect": "unchanged",
    }
    prop.update(overrides)
    return {
        "metrics": [
            {
                "name": "mql_count",
                "description": "Count of leads flagged is_mql, first touch.",
                "sql_expression": "COUNT(DISTINCT lead_id)",
                "sensitivity": [prop],
            }
        ]
    }


class TestDataModel:
    def test_metric_defaults_to_no_sensitivity(self) -> None:
        m = MetricDefinition(name="signups", description="", sql_expression="COUNT(*)")
        assert m.sensitivity == []

    def test_property_holds_claim_and_shadow(self) -> None:
        p = SensitivityProperty(
            name="p",
            description="the claim",
            shadow=Shadow(table="mkt.touchpoints", sql=SHADOW_SQL),
            expect="unchanged",
        )
        assert p.shadow.table == "mkt.touchpoints"
        assert p.expect == "unchanged"


class TestParsing:
    def test_parses_a_declared_property(self) -> None:
        metric = YamlSource.from_raw(_raw()).get_metric("mql_count")
        assert metric is not None
        (prop,) = metric.sensitivity
        assert prop.name == "attribution_is_first_touch"
        assert prop.expect == "unchanged"
        assert prop.shadow.table == "mkt.touchpoints"
        assert "UNION ALL" in prop.shadow.sql
        assert prop.description.startswith("A touchpoint after qualification")

    def test_metric_without_the_key_parses_to_empty(self) -> None:
        raw = {"metrics": [{"name": "m", "description": "", "sql_expression": "1"}]}
        metric = YamlSource.from_raw(raw).get_metric("m")
        assert metric is not None
        assert metric.sensitivity == []

    def test_non_mapping_shadow_raises(self) -> None:
        with pytest.raises(ValueError, match="shadow"):
            YamlSource.from_raw(_raw(shadow="SELECT 1"))
