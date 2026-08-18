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


from agentic_data_contracts.semantic.yaml_source import YamlSource  # noqa: E402


class TestYamlSourceLoading:
    def test_parses_decompositions_and_drill_by(self) -> None:
        raw = {
            "metrics": [
                {
                    "name": "revenue",
                    "sql_expression": "SUM(amount)",
                    "decompositions": [
                        {"operator": "product", "operands": ["paying_users", "arpu"]}
                    ],
                    "drill_by": [
                        {
                            "dimension": "region",
                            "column": "analytics.dim_customer.region",
                        }
                    ],
                },
                {"name": "paying_users", "sql_expression": "x"},
                {"name": "arpu", "sql_expression": "x"},
            ]
        }
        source = YamlSource.from_raw(raw)
        revenue = source.get_metric("revenue")
        assert revenue is not None
        assert revenue.decompositions[0].operator == "product"
        assert revenue.decompositions[0].operands == ["paying_users", "arpu"]
        assert revenue.drill_by[0].dimension == "region"

    def test_leaf_metric_has_empty_lists(self) -> None:
        source = YamlSource.from_raw(
            {"metrics": [{"name": "signups", "sql_expression": "COUNT(*)"}]}
        )
        signups = source.get_metric("signups")
        assert signups is not None
        assert signups.decompositions == []
        assert signups.drill_by == []

    def test_invalid_decomposition_raises_at_load(self) -> None:
        raw = {
            "metrics": [
                {
                    "name": "revenue",
                    "sql_expression": "x",
                    "decompositions": [
                        {"operator": "product", "operands": ["ghost", "arpu"]}
                    ],
                },
                {"name": "arpu", "sql_expression": "x"},
            ]
        }
        with pytest.raises(ValueError, match="unknown metric 'ghost'"):
            YamlSource.from_raw(raw)


from agentic_data_contracts.semantic.base import dump_semantic_source  # noqa: E402


class TestRoundtrip:
    def test_dump_then_from_raw_preserves_fields(self) -> None:
        raw = {
            "metrics": [
                {
                    "name": "revenue",
                    "sql_expression": "SUM(amount)",
                    "decompositions": [
                        {"operator": "product", "operands": ["paying_users", "arpu"]}
                    ],
                    "drill_by": [
                        {
                            "dimension": "region",
                            "column": "analytics.dim_customer.region",
                        }
                    ],
                },
                {"name": "paying_users", "sql_expression": "x"},
                {"name": "arpu", "sql_expression": "x"},
            ]
        }
        source = YamlSource.from_raw(raw)
        dumped = dump_semantic_source(source)
        rebuilt = YamlSource.from_raw(dumped)
        revenue = rebuilt.get_metric("revenue")
        assert revenue is not None
        assert revenue.decompositions[0].operands == ["paying_users", "arpu"]
        assert revenue.drill_by[0].column == "analytics.dim_customer.region"

    def test_dump_omits_empty_decomposition_fields(self) -> None:
        # A leaf metric (no decompositions/drill_by) must NOT carry empty keys
        # in the dump — keeps the frozen-contract digest byte-stable against the
        # pre-0.28 format and matches the tools layer's omit-when-empty rule.
        source = YamlSource.from_raw(
            {"metrics": [{"name": "signups", "sql_expression": "COUNT(*)"}]}
        )
        metric = dump_semantic_source(source)["metrics"][0]
        assert "decompositions" not in metric
        assert "drill_by" not in metric

    def test_dump_includes_nonempty_decomposition_fields(self) -> None:
        source = YamlSource.from_raw(
            {
                "metrics": [
                    {
                        "name": "revenue",
                        "sql_expression": "x",
                        "decompositions": [
                            {"operator": "product", "operands": ["a", "b"]}
                        ],
                        "drill_by": [{"dimension": "region", "column": "s.t.region"}],
                    },
                    {"name": "a", "sql_expression": "x"},
                    {"name": "b", "sql_expression": "x"},
                ]
            }
        )
        metric = next(
            m for m in dump_semantic_source(source)["metrics"] if m["name"] == "revenue"
        )
        assert metric["decompositions"] == [
            {"operator": "product", "operands": ["a", "b"]}
        ]
        assert metric["drill_by"] == [{"dimension": "region", "column": "s.t.region"}]


from agentic_data_contracts.semantic.base import (  # noqa: E402
    build_metric_impact_index,
    walk_metric_impacts,
)


class TestMixedGraphTraversal:
    def test_walk_identity_downstream_returns_components(self) -> None:
        edges = identity_edges_from_metrics(
            [_metric("revenue", [Decomposition("product", ["paying_users", "arpu"])])]
        )
        index = build_metric_impact_index(edges)
        walk = walk_metric_impacts(
            index, "revenue", direction="downstream", max_depth=2
        )
        assert {e.to_metric for _, e in walk} == {"paying_users", "arpu"}
        assert all(e.kind == "identity" for _, e in walk)

    def test_walk_identity_upstream_returns_parents(self) -> None:
        edges = identity_edges_from_metrics(
            [_metric("revenue", [Decomposition("product", ["paying_users", "arpu"])])]
        )
        index = build_metric_impact_index(edges)
        walk = walk_metric_impacts(index, "arpu", direction="upstream", max_depth=2)
        assert {e.from_metric for _, e in walk} == {"revenue"}

    def test_index_mixes_influence_and_identity(self) -> None:
        influence = [MetricImpact(from_metric="conv", to_metric="revenue")]
        identity = identity_edges_from_metrics(
            [_metric("revenue", [Decomposition("product", ["paying_users", "arpu"])])]
        )
        index = build_metric_impact_index([*influence, *identity])
        kinds = {e.kind for edges in index.values() for e in edges}
        assert kinds == {"influence", "identity"}


class TestConventionValidation:
    def _metrics(
        self,
        *,
        convention: str | None = None,
        convention_operand: str | None = None,
        operator: str = "product",
        operands: list[str] | None = None,
    ) -> list[MetricDefinition]:
        return [
            MetricDefinition(
                name="activations",
                description="",
                sql_expression="",
                decompositions=[
                    Decomposition(
                        operator=operator,
                        operands=operands
                        if operands is not None
                        else ["volume", "rate"],
                        convention=convention,
                        convention_operand=convention_operand,
                    )
                ],
            ),
            MetricDefinition(name="volume", description="", sql_expression=""),
            MetricDefinition(name="rate", description="", sql_expression=""),
        ]

    def test_undeclared_convention_is_valid(self) -> None:
        validate_decompositions(self._metrics())

    def test_each_vocabulary_value_is_accepted(self) -> None:
        validate_decompositions(self._metrics(convention="explicit"))
        validate_decompositions(self._metrics(convention="split_evenly"))
        validate_decompositions(
            self._metrics(convention="fold_into", convention_operand="rate")
        )

    def test_unknown_convention_raises(self) -> None:
        with pytest.raises(ValueError, match="unknown attribution convention"):
            validate_decompositions(self._metrics(convention="laspeyres"))

    def test_convention_on_sum_raises(self) -> None:
        # sum is linear: there is no cross term to place, so declaring where it
        # goes is a misunderstanding worth surfacing at authoring time.
        with pytest.raises(ValueError, match="no cross term"):
            validate_decompositions(
                self._metrics(
                    operator="sum",
                    operands=["volume", "rate"],
                    convention="split_evenly",
                )
            )

    def test_convention_on_difference_raises(self) -> None:
        with pytest.raises(ValueError, match="no cross term"):
            validate_decompositions(
                self._metrics(
                    operator="difference",
                    operands=["volume", "rate"],
                    convention="explicit",
                )
            )

    def test_fold_into_without_operand_raises(self) -> None:
        with pytest.raises(ValueError, match="requires 'convention_operand'"):
            validate_decompositions(self._metrics(convention="fold_into"))

    def test_fold_into_unknown_operand_raises(self) -> None:
        with pytest.raises(ValueError, match="is not one of its operands"):
            validate_decompositions(
                self._metrics(convention="fold_into", convention_operand="margin")
            )

    def test_convention_operand_without_fold_into_raises(self) -> None:
        with pytest.raises(ValueError, match="only meaningful with"):
            validate_decompositions(
                self._metrics(convention="split_evenly", convention_operand="rate")
            )

    def test_convention_operand_without_any_convention_raises(self) -> None:
        with pytest.raises(ValueError, match="declares no convention"):
            validate_decompositions(self._metrics(convention_operand="rate"))


class TestConventionRoundTrip:
    def test_declared_convention_survives_dump_and_reload(self) -> None:
        raw = {
            "metrics": [
                {
                    "name": "activations",
                    "decompositions": [
                        {
                            "operator": "product",
                            "operands": ["volume", "rate"],
                            "convention": "fold_into",
                            "convention_operand": "rate",
                        }
                    ],
                },
                {"name": "volume"},
                {"name": "rate"},
            ]
        }
        reloaded = YamlSource.from_raw(dump_semantic_source(YamlSource.from_raw(raw)))
        metric = reloaded.get_metric("activations")
        assert metric is not None
        assert metric.decompositions[0].convention == "fold_into"
        assert metric.decompositions[0].convention_operand == "rate"

    def test_resolved_default_survives_dump_and_reload(self) -> None:
        # The frozen artifact states the effective convention outright, rather
        # than carrying the source-level key for a consumer to re-apply.
        raw = {
            "decomposition_convention": {"convention": "split_evenly"},
            "metrics": [
                {
                    "name": "activations",
                    "decompositions": [
                        {"operator": "product", "operands": ["volume", "rate"]}
                    ],
                },
                {"name": "volume"},
                {"name": "rate"},
            ],
        }
        dumped = dump_semantic_source(YamlSource.from_raw(raw))
        assert dumped["metrics"][0]["decompositions"][0]["convention"] == "split_evenly"
        reloaded = YamlSource.from_raw(dumped)
        metric = reloaded.get_metric("activations")
        assert metric is not None
        assert metric.decompositions[0].convention == "split_evenly"

    def test_undeclared_convention_emits_no_keys(self) -> None:
        # Digest stability: a contract that declares no convention must dump
        # byte-identically to the pre-0.43 format.
        raw = {
            "metrics": [
                {
                    "name": "activations",
                    "decompositions": [
                        {"operator": "product", "operands": ["volume", "rate"]}
                    ],
                },
                {"name": "volume"},
                {"name": "rate"},
            ]
        }
        dumped = dump_semantic_source(YamlSource.from_raw(raw))
        assert dumped["metrics"][0]["decompositions"][0] == {
            "operator": "product",
            "operands": ["volume", "rate"],
        }


class TestIdentityEdgeConvention:
    def test_every_operand_edge_carries_the_convention(self) -> None:
        # The convention is a property of the identity, and an edge is one
        # operand of it, so all edges from one decomposition share the pair.
        metrics = [
            MetricDefinition(
                name="activations",
                description="",
                sql_expression="",
                decompositions=[
                    Decomposition(
                        operator="product",
                        operands=["volume", "rate"],
                        convention="fold_into",
                        convention_operand="rate",
                    )
                ],
            )
        ]
        edges = identity_edges_from_metrics(metrics)
        assert len(edges) == 2
        assert all(e.convention == "fold_into" for e in edges)
        assert all(e.convention_operand == "rate" for e in edges)

    def test_undeclared_convention_leaves_edges_unset(self) -> None:
        metrics = [
            MetricDefinition(
                name="net",
                description="",
                sql_expression="",
                decompositions=[Decomposition(operator="sum", operands=["a", "b"])],
            )
        ]
        edges = identity_edges_from_metrics(metrics)
        assert all(e.convention is None for e in edges)
        assert all(e.convention_operand is None for e in edges)
