"""Tests for MetricImpact dataclass, index builder, and BFS walker."""

from __future__ import annotations

from pathlib import Path

import pytest

from agentic_data_contracts.semantic.base import (
    MetricImpact,
    build_metric_impact_index,
    walk_metric_impacts,
)
from agentic_data_contracts.semantic.cube import CubeSource
from agentic_data_contracts.semantic.dbt import DbtSource
from agentic_data_contracts.semantic.yaml_source import YamlSource


class TestMetricImpactDataclass:
    def test_defaults(self) -> None:
        imp = MetricImpact(from_metric="a", to_metric="b")
        assert imp.direction == "positive"
        assert imp.confidence == "hypothesized"
        assert imp.evidence == ""
        assert imp.description == ""

    def test_full(self) -> None:
        imp = MetricImpact(
            from_metric="conv",
            to_metric="rev",
            direction="negative",
            confidence="verified",
            evidence="study Q4",
            description="elaborate note",
        )
        assert imp.direction == "negative"
        assert imp.confidence == "verified"
        assert imp.evidence == "study Q4"


class TestBuildMetricImpactIndex:
    def test_indexes_both_endpoints(self) -> None:
        impacts = [MetricImpact(from_metric="conv", to_metric="rev")]
        index = build_metric_impact_index(impacts)
        assert len(index["conv"]) == 1
        assert len(index["rev"]) == 1
        assert index["conv"][0] is index["rev"][0]

    def test_self_loop_not_duplicated(self) -> None:
        impacts = [MetricImpact(from_metric="x", to_metric="x")]
        index = build_metric_impact_index(impacts)
        assert len(index["x"]) == 1

    def test_empty(self) -> None:
        assert build_metric_impact_index([]) == {}

    def test_multiple_edges_to_same_target(self) -> None:
        impacts = [
            MetricImpact(from_metric="conv", to_metric="rev"),
            MetricImpact(from_metric="aov", to_metric="rev"),
            MetricImpact(from_metric="traffic", to_metric="rev"),
        ]
        index = build_metric_impact_index(impacts)
        assert len(index["rev"]) == 3


class TestWalkMetricImpacts:
    def _chain(self) -> list[MetricImpact]:
        # traffic -> conv -> rev  (two-hop driver chain)
        return [
            MetricImpact(from_metric="traffic", to_metric="conv"),
            MetricImpact(from_metric="conv", to_metric="rev"),
        ]

    def test_downstream_direct(self) -> None:
        index = build_metric_impact_index(self._chain())
        walk = walk_metric_impacts(index, "traffic", direction="downstream")
        targets = [e.to_metric for _, e in walk]
        assert "conv" in targets
        assert "rev" in targets

    def test_upstream_two_hops(self) -> None:
        index = build_metric_impact_index(self._chain())
        walk = walk_metric_impacts(index, "rev", direction="upstream", max_depth=2)
        froms = {e.from_metric for _, e in walk}
        assert froms == {"conv", "traffic"}

    def test_max_depth_enforced(self) -> None:
        index = build_metric_impact_index(self._chain())
        walk = walk_metric_impacts(index, "rev", direction="upstream", max_depth=1)
        froms = {e.from_metric for _, e in walk}
        assert froms == {"conv"}  # does not reach "traffic"

    def test_depth_annotation(self) -> None:
        index = build_metric_impact_index(self._chain())
        walk = walk_metric_impacts(index, "rev", direction="upstream", max_depth=2)
        by_from = {e.from_metric: d for d, e in walk}
        assert by_from["conv"] == 1
        assert by_from["traffic"] == 2

    def test_cycle_visited_tracking(self) -> None:
        # a -> b -> c -> a (cycle)
        impacts = [
            MetricImpact(from_metric="a", to_metric="b"),
            MetricImpact(from_metric="b", to_metric="c"),
            MetricImpact(from_metric="c", to_metric="a"),
        ]
        index = build_metric_impact_index(impacts)
        walk = walk_metric_impacts(index, "a", direction="downstream", max_depth=10)
        visited_targets = [e.to_metric for _, e in walk]
        # Each target appears at most once — no infinite loop.
        assert len(visited_targets) == len(set(visited_targets))
        assert "a" not in visited_targets  # start is not revisited

    def test_invalid_direction(self) -> None:
        with pytest.raises(ValueError, match="upstream.*downstream"):
            walk_metric_impacts({}, "x", direction="sideways")

    def test_missing_start_returns_empty(self) -> None:
        index = build_metric_impact_index(self._chain())
        assert walk_metric_impacts(index, "unknown", direction="upstream") == []

    def test_direction_respects_edge_orientation(self) -> None:
        """Downstream must only follow edges where from_metric=current."""
        impacts = [
            MetricImpact(from_metric="a", to_metric="b"),
            MetricImpact(from_metric="c", to_metric="a"),  # incoming to `a`
        ]
        index = build_metric_impact_index(impacts)
        walk = walk_metric_impacts(index, "a", direction="downstream")
        targets = {e.to_metric for _, e in walk}
        assert targets == {"b"}  # does not follow c->a incoming edge

    def test_self_loop_end_to_end(self) -> None:
        """Index builder dedups self-loops; walker then yields no neighbors
        because the sole edge's neighbor is `start`, already visited."""
        index = build_metric_impact_index(
            [MetricImpact(from_metric="a", to_metric="a")]
        )
        assert walk_metric_impacts(index, "a", direction="downstream") == []
        assert walk_metric_impacts(index, "a", direction="upstream") == []


class TestYamlSourceImpacts:
    def test_loads_metric_impacts(self, fixtures_dir: Path) -> None:
        source = YamlSource(fixtures_dir / "semantic_source.yml")
        impacts = source.get_metric_impacts()
        assert len(impacts) == 1
        imp = impacts[0]
        assert imp.from_metric == "active_customers"
        assert imp.to_metric == "total_revenue"
        assert imp.direction == "positive"
        assert imp.confidence == "verified"
        assert "exp-042" in imp.evidence

    def test_loads_new_metric_fields(self, fixtures_dir: Path) -> None:
        source = YamlSource(fixtures_dir / "semantic_source.yml")
        rev = source.get_metric("total_revenue")
        assert rev is not None
        assert rev.domains == ["revenue"]
        assert "north_star" in rev.tier
        assert rev.indicator_kind == "lagging"

        ac = source.get_metric("active_customers")
        assert ac is not None
        assert ac.domains == ["engagement", "revenue"]
        assert ac.tier == ["team_kpi"]
        assert ac.indicator_kind == "leading"

    def test_no_impact_block_defaults_empty(self, tmp_path: Path) -> None:
        (tmp_path / "empty.yml").write_text("metrics: []\n")
        source = YamlSource(tmp_path / "empty.yml")
        assert source.get_metric_impacts() == []

    def test_impact_defaults_when_minimal(self, tmp_path: Path) -> None:
        yml = (
            "metrics:\n"
            "  - name: a\n"
            '    description: ""\n'
            '    sql_expression: ""\n'
            "  - name: b\n"
            '    description: ""\n'
            '    sql_expression: ""\n'
            "metric_impacts:\n"
            "  - from: a\n"
            "    to: b\n"
        )
        (tmp_path / "m.yml").write_text(yml)
        source = YamlSource(tmp_path / "m.yml")
        impacts = source.get_metric_impacts()
        assert len(impacts) == 1
        imp = impacts[0]
        assert imp.direction == "positive"
        assert imp.confidence == "hypothesized"
        assert imp.evidence == ""


class TestDbtAndCubeImpactsDefaultEmpty:
    def test_dbt(self, fixtures_dir: Path) -> None:
        source = DbtSource(fixtures_dir / "sample_dbt_manifest.json")
        assert source.get_metric_impacts() == []

    def test_cube(self, fixtures_dir: Path) -> None:
        source = CubeSource(fixtures_dir / "sample_cube_schema.yml")
        assert source.get_metric_impacts() == []


class TestDbtAndCubeMetaFields:
    def test_dbt_reads_tier_and_domains_from_meta(self, tmp_path: Path) -> None:
        manifest = {
            "nodes": {},
            "metrics": {
                "metric.project.revenue": {
                    "unique_id": "metric.project.revenue",
                    "name": "revenue",
                    "description": "Test metric",
                    "type_params": {"measure": {"expr": "SUM(amount)"}},
                    "meta": {
                        "tier": ["north_star"],
                        "indicator_kind": "lagging",
                        "domains": ["Revenue"],
                    },
                }
            },
        }
        import json as _json

        path = tmp_path / "manifest.json"
        path.write_text(_json.dumps(manifest))
        source = DbtSource(path)
        m = source.get_metric("revenue")
        assert m is not None
        assert m.tier == ["north_star"]
        assert m.indicator_kind == "lagging"
        assert m.domains == ["Revenue"]

    def test_dbt_tier_string_coerced_to_list(self, tmp_path: Path) -> None:
        manifest = {
            "nodes": {},
            "metrics": {
                "metric.x": {
                    "unique_id": "metric.x",
                    "name": "x",
                    "description": "",
                    "type_params": {"measure": {"expr": "COUNT(*)"}},
                    "meta": {"tier": "team_kpi"},
                }
            },
        }
        import json as _json

        path = tmp_path / "m.json"
        path.write_text(_json.dumps(manifest))
        source = DbtSource(path)
        m = source.get_metric("x")
        assert m is not None
        assert m.tier == ["team_kpi"]

    def test_cube_tier_string_coerced_to_list(self, tmp_path: Path) -> None:
        yml = (
            "cubes:\n"
            "  - name: Orders\n"
            "    sql_table: analytics.orders\n"
            "    measures:\n"
            "      - name: revenue\n"
            '        sql: "SUM(amount)"\n'
            "        meta:\n"
            "          tier: team_kpi\n"
            "          domains: Revenue\n"
        )
        path = tmp_path / "cube.yml"
        path.write_text(yml)
        source = CubeSource(path)
        m = source.get_metric("revenue")
        assert m is not None
        assert m.tier == ["team_kpi"]
        assert m.domains == ["Revenue"]

    def test_yaml_tier_string_coerced_to_list(self, tmp_path: Path) -> None:
        yml = (
            "metrics:\n"
            "  - name: x\n"
            '    description: ""\n'
            '    sql_expression: ""\n'
            "    tier: north_star\n"
            "    domains: Revenue\n"
        )
        path = tmp_path / "m.yml"
        path.write_text(yml)
        source = YamlSource(path)
        m = source.get_metric("x")
        assert m is not None
        assert m.tier == ["north_star"]
        assert m.domains == ["Revenue"]

    def test_cube_reads_meta_fields(self, tmp_path: Path) -> None:
        yml = (
            "cubes:\n"
            "  - name: Orders\n"
            "    sql_table: analytics.orders\n"
            "    measures:\n"
            "      - name: revenue\n"
            '        sql: "SUM(amount)"\n'
            "        meta:\n"
            "          tier: [north_star]\n"
            "          indicator_kind: lagging\n"
            "          domains: [Revenue]\n"
        )
        path = tmp_path / "cube.yml"
        path.write_text(yml)
        source = CubeSource(path)
        m = source.get_metric("revenue")
        assert m is not None
        assert m.tier == ["north_star"]
        assert m.indicator_kind == "lagging"
        assert m.domains == ["Revenue"]

    def test_cube_no_meta_leaves_defaults(self, fixtures_dir: Path) -> None:
        source = CubeSource(fixtures_dir / "sample_cube_schema.yml")
        m = source.get_metric("total_revenue")
        assert m is not None
        assert m.tier == []
        assert m.indicator_kind is None
        assert m.domains == []
