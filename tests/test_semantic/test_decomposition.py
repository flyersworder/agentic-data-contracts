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


from agentic_data_contracts.semantic.base import (  # noqa: E402
    IdentityEdge,
    identity_edges_from_metrics,
)


def _metric(
    name: str, decompositions: list[Decomposition] | None = None
) -> MetricDefinition:
    return MetricDefinition(
        name=name,
        description="",
        sql_expression="x",
        decompositions=decompositions or [],
    )


class TestIdentityEdges:
    def test_edge_kind_is_identity(self) -> None:
        assert (
            IdentityEdge(
                from_metric="revenue", to_metric="arpu", operator="product"
            ).kind
            == "identity"
        )

    def test_fans_out_one_edge_per_operand(self) -> None:
        metrics = [
            _metric(
                "revenue",
                [Decomposition(operator="product", operands=["paying_users", "arpu"])],
            ),
            _metric("paying_users"),
            _metric("arpu"),
        ]
        edges = identity_edges_from_metrics(metrics)
        assert {(e.from_metric, e.to_metric, e.operator) for e in edges} == {
            ("revenue", "paying_users", "product"),
            ("revenue", "arpu", "product"),
        }

    def test_multiple_decompositions_all_contribute(self) -> None:
        metrics = [
            _metric(
                "revenue",
                [
                    Decomposition(
                        operator="product", operands=["paying_users", "arpu"]
                    ),
                    Decomposition(
                        operator="sum", operands=["new_revenue", "expansion_revenue"]
                    ),
                ],
            ),
        ]
        edges = identity_edges_from_metrics(metrics)
        assert len(edges) == 4

    def test_leaf_metric_produces_no_edges(self) -> None:
        assert identity_edges_from_metrics([_metric("signups")]) == []
