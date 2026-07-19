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


import pytest  # noqa: E402

from agentic_data_contracts.semantic.base import validate_decompositions  # noqa: E402


def _m(
    name: str, decompositions: list[Decomposition] | None = None
) -> MetricDefinition:
    return MetricDefinition(
        name=name,
        description="",
        sql_expression="x",
        decompositions=decompositions or [],
    )


class TestValidateDecompositions:
    def test_valid_tree_passes(self) -> None:
        metrics = [
            _m("revenue", [Decomposition("product", ["paying_users", "arpu"])]),
            _m("paying_users"),
            _m("arpu"),
        ]
        validate_decompositions(metrics)  # no raise

    def test_leaf_only_passes(self) -> None:
        validate_decompositions([_m("signups")])

    def test_unknown_operator_raises(self) -> None:
        metrics = [
            _m("revenue", [Decomposition("divide", ["a", "b"])]),
            _m("a"),
            _m("b"),
        ]
        with pytest.raises(ValueError, match="unknown operator"):
            validate_decompositions(metrics)

    def test_ratio_requires_exactly_two_operands(self) -> None:
        metrics = [
            _m("rate", [Decomposition("ratio", ["a", "b", "c"])]),
            _m("a"),
            _m("b"),
            _m("c"),
        ]
        with pytest.raises(ValueError, match="exactly 2 operands"):
            validate_decompositions(metrics)

    def test_sum_requires_at_least_two_operands(self) -> None:
        metrics = [_m("revenue", [Decomposition("sum", ["a"])]), _m("a")]
        with pytest.raises(ValueError, match="at least 2 operands"):
            validate_decompositions(metrics)

    def test_unresolved_operand_raises(self) -> None:
        metrics = [
            _m("revenue", [Decomposition("product", ["paying_users", "ghost"])]),
            _m("paying_users"),
        ]
        with pytest.raises(ValueError, match="unknown metric 'ghost'"):
            validate_decompositions(metrics)

    def test_self_reference_raises(self) -> None:
        metrics = [
            _m("revenue", [Decomposition("sum", ["revenue", "arpu"])]),
            _m("arpu"),
        ]
        with pytest.raises(ValueError, match="itself"):
            validate_decompositions(metrics)

    def test_two_cycle_raises(self) -> None:
        metrics = [
            _m("a", [Decomposition("sum", ["b", "c"])]),
            _m("b", [Decomposition("sum", ["a", "c"])]),
            _m("c"),
        ]
        with pytest.raises(ValueError, match="cycle"):
            validate_decompositions(metrics)

    def test_diamond_is_not_a_cycle(self) -> None:
        # a -> b, a -> c, b -> d, c -> d : shared child, no cycle
        metrics = [
            _m("a", [Decomposition("sum", ["b", "c"])]),
            _m("b", [Decomposition("sum", ["d", "e"])]),
            _m("c", [Decomposition("sum", ["d", "e"])]),
            _m("d"),
            _m("e"),
        ]
        validate_decompositions(metrics)  # no raise


from agentic_data_contracts.adapters.base import Column, TableSchema  # noqa: E402
from agentic_data_contracts.semantic.base import validate_drill_by  # noqa: E402


def _md(name: str, drill_by: list[DrillDimension]) -> MetricDefinition:
    return MetricDefinition(
        name=name, description="", sql_expression="x", drill_by=drill_by
    )


class TestValidateDrillBy:
    def test_declared_column_passes(self) -> None:
        schemas = {
            "analytics.dim_customer": TableSchema(
                columns=[Column(name="region", type="VARCHAR")]
            )
        }
        metrics = [
            _md("revenue", [DrillDimension("region", "analytics.dim_customer.region")])
        ]
        validate_drill_by(metrics, schemas)  # no raise

    def test_unknown_column_in_declared_table_raises(self) -> None:
        schemas = {
            "analytics.dim_customer": TableSchema(
                columns=[Column(name="region", type="VARCHAR")]
            )
        }
        metrics = [
            _md(
                "revenue", [DrillDimension("segment", "analytics.dim_customer.segment")]
            )
        ]
        with pytest.raises(ValueError, match="unknown column"):
            validate_drill_by(metrics, schemas)

    def test_undeclared_table_is_skipped_silently(self) -> None:
        metrics = [_md("revenue", [DrillDimension("plan", "analytics.dim_plan.tier")])]
        validate_drill_by(metrics, {})  # table not declared -> soft skip, no raise

    def test_malformed_column_raises(self) -> None:
        metrics = [_md("revenue", [DrillDimension("region", "region")])]
        with pytest.raises(ValueError, match="schema.table.column"):
            validate_drill_by(metrics, {})
