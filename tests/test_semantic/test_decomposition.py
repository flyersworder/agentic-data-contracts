"""Tests for metric identity decomposition + drill dimensions."""

from __future__ import annotations

from agentic_data_contracts.semantic.base import (
    Decomposition,
    DrillDimension,
    MetricDefinition,
    MetricImpact,
)


class TestDataModel:
    def test_metric_defaults_to_no_decomposition(self) -> None:
        m = MetricDefinition(name="signups", description="", sql_expression="COUNT(*)")
        assert m.decompositions == []
        assert m.drill_by == []

    def test_decomposition_holds_operator_and_operands(self) -> None:
        d = Decomposition(operator="product", operands=["paying_users", "arpu"])
        assert d.operator == "product"
        assert d.operands == ["paying_users", "arpu"]

    def test_drill_dimension_holds_dimension_and_column(self) -> None:
        dd = DrillDimension(dimension="region", column="analytics.dim_customer.region")
        assert dd.dimension == "region"
        assert dd.column == "analytics.dim_customer.region"

    def test_metric_impact_kind_is_influence(self) -> None:
        assert MetricImpact(from_metric="a", to_metric="b").kind == "influence"
