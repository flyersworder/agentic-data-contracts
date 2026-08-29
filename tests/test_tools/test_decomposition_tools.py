"""Integration tests for decomposition surfacing on the tool layer."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.schema import (
    AllowedTable,
    DataContractSchema,
    SemanticConfig,
)
from agentic_data_contracts.semantic.yaml_source import YamlSource
from agentic_data_contracts.tools.factory import create_tools

FIXTURE = Path(__file__).parent.parent / "fixtures" / "decomposition_source.yml"


def _tools() -> dict:
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate(
                    {"schema": "analytics", "tables": ["dim_customer"]}
                ),
            ],
        ),
    )
    contract = DataContract(schema)
    source = YamlSource(FIXTURE)
    tools = create_tools(contract, semantic_source=source)
    return {t.name: t.callable for t in tools}


async def _call(fn, **args) -> dict:
    result = await fn(args)
    return json.loads(result["content"][0]["text"])


class TestLookupMetricSurfacesDecomposition:
    @pytest.mark.asyncio
    async def test_includes_decompositions_and_drill_by(self) -> None:
        data = await _call(_tools()["lookup_metric"], metric_name="revenue")
        assert {
            "operator": "product",
            "operands": ["paying_users", "arpu"],
            "convention": "fold_into",
            "convention_operand": "arpu",
        } in data["decompositions"]
        assert {
            "dimension": "region",
            "column": "analytics.dim_customer.region",
        } in data["drill_by"]

    @pytest.mark.asyncio
    async def test_leaf_metric_omits_fields(self) -> None:
        data = await _call(_tools()["lookup_metric"], metric_name="paying_users")
        assert "decompositions" not in data
        assert "drill_by" not in data


class TestTraceWalksIdentity:
    @pytest.mark.asyncio
    async def test_upstream_all_includes_identity_and_influence(self) -> None:
        data = await _call(
            _tools()["trace_metric_impacts"],
            metric_name="revenue",
            direction="upstream",
        )
        kinds = {e["kind"] for e in data["edges"]}
        assert "identity" in kinds
        identity_edges = [e for e in data["edges"] if e["kind"] == "identity"]
        assert any(
            e["from"] == "arpu" and e["operator"] == "product" for e in identity_edges
        )

    @pytest.mark.asyncio
    async def test_kinds_identity_excludes_influence(self) -> None:
        # Walk downstream from arpu: arpu is an operand of revenue, so it
        # feeds revenue; arpu has no influence edges, so kinds="identity"
        # must return a non-empty, all-identity set.
        data = await _call(
            _tools()["trace_metric_impacts"],
            metric_name="arpu",
            direction="downstream",
            kinds="identity",
        )
        assert data["edges"]  # non-empty
        assert all(e["kind"] == "identity" for e in data["edges"])
        assert any(e["to"] == "revenue" for e in data["edges"])

    @pytest.mark.asyncio
    async def test_kinds_influence_excludes_identity(self) -> None:
        data = await _call(
            _tools()["trace_metric_impacts"],
            metric_name="revenue",
            direction="upstream",
            kinds="influence",
        )
        assert all(e["kind"] == "influence" for e in data["edges"])

    @pytest.mark.asyncio
    async def test_invalid_kinds_returns_message(self) -> None:
        result = await _tools()["trace_metric_impacts"](
            {"metric_name": "revenue", "kinds": "bogus"}
        )
        assert "kinds must be" in result["content"][0]["text"]


class TestConventionDelivery:
    @pytest.mark.asyncio
    async def test_lookup_metric_omits_an_undeclared_convention(self) -> None:
        # revenue's `sum` decomposition declares none: the keys must be absent,
        # not present-and-null. An always-present key would move every
        # published contract digest.
        data = await _call(_tools()["lookup_metric"], metric_name="revenue")
        sums = [d for d in data["decompositions"] if d["operator"] == "sum"]
        assert sums
        assert "convention" not in sums[0]
        assert "convention_operand" not in sums[0]

    @pytest.mark.asyncio
    async def test_trace_identity_edges_carry_the_convention(self) -> None:
        # trace_metric_impacts' own description tells the agent to walk
        # 'identity' first for root cause -- that is the attribution workflow.
        # It must not hand over the operator without the convention.
        data = await _call(
            _tools()["trace_metric_impacts"],
            metric_name="arpu",
            direction="downstream",
            kinds="identity",
        )
        product_edges = [e for e in data["edges"] if e["operator"] == "product"]
        assert product_edges
        for edge in product_edges:
            assert edge["convention"] == "fold_into"
            assert edge["convention_operand"] == "arpu"

    @pytest.mark.asyncio
    async def test_trace_omits_an_undeclared_convention(self) -> None:
        data = await _call(
            _tools()["trace_metric_impacts"],
            metric_name="new_revenue",
            direction="downstream",
            kinds="identity",
        )
        sum_edges = [e for e in data["edges"] if e["operator"] == "sum"]
        assert sum_edges
        assert all("convention" not in e for e in sum_edges)
        assert all("convention_operand" not in e for e in sum_edges)


class TestDirectionMeansDriversForBothKinds:
    """`direction` asks a question about metrics, not about edge tuples.

    The fixture declares `paying_users -> revenue` twice: as a `metric_impacts`
    influence edge, and as an operand of revenue's `product` decomposition. It
    is one real relationship, so one direction must return both.
    """

    @pytest.mark.asyncio
    async def test_the_same_pair_arrives_under_one_direction(self) -> None:
        data = await _call(
            _tools()["trace_metric_impacts"],
            metric_name="revenue",
            direction="upstream",
        )
        pairs = {(e["kind"], e["from"], e["to"]) for e in data["edges"]}
        assert ("influence", "paying_users", "revenue") in pairs
        assert ("identity", "paying_users", "revenue") in pairs

    @pytest.mark.asyncio
    async def test_upstream_identity_returns_the_operands_that_drive_it(self) -> None:
        data = await _call(
            _tools()["trace_metric_impacts"],
            metric_name="revenue",
            direction="upstream",
            kinds="identity",
            max_depth=1,
        )
        assert {e["from"] for e in data["edges"]} == {
            "paying_users",
            "arpu",
            "new_revenue",
            "expansion_revenue",
        }
        assert all(e["to"] == "revenue" for e in data["edges"])

    @pytest.mark.asyncio
    async def test_downstream_identity_returns_the_parent_it_feeds(self) -> None:
        data = await _call(
            _tools()["trace_metric_impacts"],
            metric_name="arpu",
            direction="downstream",
            kinds="identity",
        )
        assert [(e["from"], e["to"]) for e in data["edges"]] == [("arpu", "revenue")]

    @pytest.mark.asyncio
    async def test_a_multi_hop_driver_walk_keeps_pointing_the_same_way(self) -> None:
        # new_revenue drives revenue and is itself driven by trial_conversions,
        # so depth 2 must arrive still oriented driver -> affected.
        data = await _call(
            _tools()["trace_metric_impacts"],
            metric_name="revenue",
            direction="upstream",
            kinds="identity",
            max_depth=2,
        )
        deep = [(e["from"], e["to"]) for e in data["edges"] if e["depth"] == 2]
        assert ("trial_conversions", "new_revenue") in deep


class TestEmptyIdentityWalkDisclosesTheOtherDirection:
    """An empty payload must not read as "this metric declares nothing".

    That silence is what #81 was about, and this release re-points identity
    edges, so a caller who copied the old README example asks the direction
    that is now empty. Say where the edges are instead of returning bare `[]`.
    """

    @pytest.mark.asyncio
    async def test_names_the_direction_that_has_edges(self) -> None:
        # revenue is nothing's operand, so it drives no metric identically --
        # but four operands drive it.
        data = await _call(
            _tools()["trace_metric_impacts"],
            metric_name="revenue",
            direction="downstream",
            kinds="identity",
        )
        assert data["edges"] == []
        assert "upstream" in data["note"]

    @pytest.mark.asyncio
    async def test_stays_quiet_when_the_walk_found_something(self) -> None:
        data = await _call(
            _tools()["trace_metric_impacts"],
            metric_name="revenue",
            direction="upstream",
            kinds="identity",
        )
        assert data["edges"]
        assert "note" not in data

    @pytest.mark.asyncio
    async def test_stays_quiet_on_an_influence_only_walk(self) -> None:
        # The caller excluded identity edges, so their whereabouts are not an
        # answer to the question asked.
        data = await _call(
            _tools()["trace_metric_impacts"],
            metric_name="revenue",
            direction="downstream",
            kinds="influence",
        )
        assert data["edges"] == []
        assert "note" not in data

    @pytest.mark.asyncio
    async def test_counts_metrics_rather_than_edges(self) -> None:
        # trial_conversions is an operand of new_revenue in two separate
        # decompositions, so two edges lead to one metric. The sentence says
        # "metrics", so the number has to be a metric count.
        data = await _call(
            _tools()["trace_metric_impacts"],
            metric_name="trial_conversions",
            direction="upstream",
            kinds="identity",
        )
        assert data["edges"] == []
        assert "1 downstream" in data["note"]
