"""Tests for metric-impact enrichment in list_metrics/lookup_metric and the
trace_metric_impacts tool."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.schema import (
    AllowedTable,
    DataContractSchema,
    SemanticConfig,
)
from agentic_data_contracts.semantic.base import (
    Decomposition,
    MetricDefinition,
    MetricImpact,
    Relationship,
    TableSchema,
)
from agentic_data_contracts.semantic.yaml_source import YamlSource
from agentic_data_contracts.tools.factory import create_tools


@pytest.fixture
def semantic(fixtures_dir: Path) -> YamlSource:
    return YamlSource(fixtures_dir / "semantic_source.yml")


@pytest.fixture
def contract_no_domains(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml(fixtures_dir / "valid_contract.yml")


# ──────────────────────────────────────────────────────────────────────────
# lookup_metric enrichment
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_lookup_metric_exact_returns_tier_and_indicator(
    contract_no_domains: DataContract, semantic: YamlSource
) -> None:
    tools = create_tools(contract_no_domains, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "lookup_metric")
    result = await tool.callable({"metric_name": "total_revenue"})
    data = json.loads(result["content"][0]["text"])
    assert data["tier"] == ["north_star", "department_kpi"]
    assert data["indicator_kind"] == "lagging"
    assert "revenue" in data["domains"]


@pytest.mark.asyncio
async def test_lookup_metric_exact_returns_impacted_by(
    contract_no_domains: DataContract, semantic: YamlSource
) -> None:
    tools = create_tools(contract_no_domains, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "lookup_metric")
    result = await tool.callable({"metric_name": "total_revenue"})
    data = json.loads(result["content"][0]["text"])
    assert "impacted_by" in data
    assert any("active_customers" in s for s in data["impacted_by"])
    assert any("verified" in s for s in data["impacted_by"])
    assert any("exp-042" in s for s in data["impacted_by"])


@pytest.mark.asyncio
async def test_lookup_metric_exact_returns_outgoing_impacts(
    contract_no_domains: DataContract, semantic: YamlSource
) -> None:
    tools = create_tools(contract_no_domains, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "lookup_metric")
    result = await tool.callable({"metric_name": "active_customers"})
    data = json.loads(result["content"][0]["text"])
    assert "impacts" in data
    assert any("total_revenue" in s for s in data["impacts"])


@pytest.mark.asyncio
async def test_lookup_metric_fuzzy_candidates_are_enriched(
    contract_no_domains: DataContract, semantic: YamlSource
) -> None:
    tools = create_tools(contract_no_domains, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "lookup_metric")
    result = await tool.callable({"metric_name": "revenue from orders"})
    data = json.loads(result["content"][0]["text"])
    assert data["exact_match"] is False
    # At least one candidate should carry enrichment fields.
    rev = next((c for c in data["candidates"] if c["name"] == "total_revenue"), None)
    assert rev is not None
    assert rev["tier"] == ["north_star", "department_kpi"]


# ──────────────────────────────────────────────────────────────────────────
# list_metrics enrichment and filters
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_list_metrics_entries_include_tier(
    contract_no_domains: DataContract, semantic: YamlSource
) -> None:
    tools = create_tools(contract_no_domains, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "list_metrics")
    result = await tool.callable({})
    data = json.loads(result["content"][0]["text"])
    by_name = {m["name"]: m for m in data["metrics"]}
    assert "north_star" in by_name["total_revenue"]["tier"]
    assert by_name["active_customers"]["indicator_kind"] == "leading"


@pytest.mark.asyncio
async def test_list_metrics_filter_by_tier(
    contract_no_domains: DataContract, semantic: YamlSource
) -> None:
    tools = create_tools(contract_no_domains, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "list_metrics")
    result = await tool.callable({"tier": "north_star"})
    data = json.loads(result["content"][0]["text"])
    names = [m["name"] for m in data["metrics"]]
    assert names == ["total_revenue"]


@pytest.mark.asyncio
async def test_list_metrics_filter_by_indicator_kind(
    contract_no_domains: DataContract, semantic: YamlSource
) -> None:
    tools = create_tools(contract_no_domains, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "list_metrics")
    result = await tool.callable({"indicator_kind": "leading"})
    data = json.loads(result["content"][0]["text"])
    names = [m["name"] for m in data["metrics"]]
    assert names == ["active_customers"]


@pytest.mark.asyncio
async def test_list_metrics_filter_by_metric_declared_domain(
    contract_no_domains: DataContract, semantic: YamlSource
) -> None:
    """Even without a contract Domain, the metric's self-declared `domains`
    should be discoverable via list_metrics(domain=...)."""
    tools = create_tools(contract_no_domains, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "list_metrics")
    result = await tool.callable({"domain": "engagement"})
    data = json.loads(result["content"][0]["text"])
    names = [m["name"] for m in data["metrics"]]
    assert names == ["active_customers"]


@pytest.mark.asyncio
async def test_list_metrics_unknown_domain_still_errors(
    contract_no_domains: DataContract, semantic: YamlSource
) -> None:
    tools = create_tools(contract_no_domains, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "list_metrics")
    result = await tool.callable({"domain": "nowhere_land"})
    text = result["content"][0]["text"]
    assert "not found" in text.lower()


# ──────────────────────────────────────────────────────────────────────────
# Factory validation warnings
# ──────────────────────────────────────────────────────────────────────────


def test_factory_warns_on_unknown_impact_ref(
    fixtures_dir: Path,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    yml = (
        "metrics:\n"
        "  - name: total_revenue\n"
        '    description: ""\n'
        '    sql_expression: ""\n'
        "metric_impacts:\n"
        "  - from: total_revenue\n"
        "    to: ghost_metric\n"
    )
    source_path = tmp_path / "s.yml"
    source_path.write_text(yml)
    source = YamlSource(source_path)

    schema = DataContractSchema(
        name="t",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate(
                    {"schema": "analytics", "tables": ["orders"]}
                ),
            ],
        ),
    )
    dc = DataContract(schema)

    with caplog.at_level(logging.WARNING):
        create_tools(dc, semantic_source=source)

    assert any("ghost_metric" in m for m in caplog.messages)


# ──────────────────────────────────────────────────────────────────────────
# trace_metric_impacts tool
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_trace_metric_impacts_upstream(
    contract_no_domains: DataContract, semantic: YamlSource
) -> None:
    tools = create_tools(contract_no_domains, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "trace_metric_impacts")
    result = await tool.callable(
        {"metric_name": "total_revenue", "direction": "upstream"}
    )
    data = json.loads(result["content"][0]["text"])
    assert data["direction"] == "upstream"
    assert data["edges"]
    edge = data["edges"][0]
    assert edge["from"] == "active_customers"
    assert edge["to"] == "total_revenue"
    assert edge["depth"] == 1
    assert edge["confidence"] == "verified"
    assert "exp-042" in edge["evidence"]


@pytest.mark.asyncio
async def test_trace_metric_impacts_downstream(
    contract_no_domains: DataContract, semantic: YamlSource
) -> None:
    tools = create_tools(contract_no_domains, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "trace_metric_impacts")
    result = await tool.callable(
        {"metric_name": "active_customers", "direction": "downstream"}
    )
    data = json.loads(result["content"][0]["text"])
    edges = data["edges"]
    assert any(e["to"] == "total_revenue" for e in edges)


@pytest.mark.asyncio
async def test_trace_metric_impacts_unknown_metric(
    contract_no_domains: DataContract, semantic: YamlSource
) -> None:
    tools = create_tools(contract_no_domains, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "trace_metric_impacts")
    result = await tool.callable(
        {"metric_name": "nonexistent", "direction": "upstream"}
    )
    text = result["content"][0]["text"]
    assert "not found" in text.lower()


@pytest.mark.asyncio
async def test_trace_metric_impacts_invalid_direction(
    contract_no_domains: DataContract, semantic: YamlSource
) -> None:
    tools = create_tools(contract_no_domains, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "trace_metric_impacts")
    result = await tool.callable(
        {"metric_name": "total_revenue", "direction": "sideways"}
    )
    text = result["content"][0]["text"]
    assert "upstream" in text and "downstream" in text


@pytest.mark.asyncio
async def test_trace_metric_impacts_respects_max_depth(
    contract_no_domains: DataContract, semantic: YamlSource
) -> None:
    tools = create_tools(contract_no_domains, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "trace_metric_impacts")
    # With only one edge in the fixture, max_depth=1 already covers it.
    result = await tool.callable(
        {"metric_name": "total_revenue", "direction": "upstream", "max_depth": 1}
    )
    data = json.loads(result["content"][0]["text"])
    assert data["max_depth"] == 1
    assert len(data["edges"]) == 1


@pytest.mark.asyncio
async def test_trace_metric_impacts_clamps_max_depth_upper_bound(
    contract_no_domains: DataContract, semantic: YamlSource
) -> None:
    tools = create_tools(contract_no_domains, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "trace_metric_impacts")
    result = await tool.callable(
        {"metric_name": "total_revenue", "direction": "upstream", "max_depth": 999}
    )
    data = json.loads(result["content"][0]["text"])
    assert data["max_depth"] == 10


@pytest.mark.asyncio
async def test_trace_metric_impacts_clamps_max_depth_lower_bound(
    contract_no_domains: DataContract, semantic: YamlSource
) -> None:
    tools = create_tools(contract_no_domains, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "trace_metric_impacts")
    result = await tool.callable(
        {"metric_name": "total_revenue", "direction": "upstream", "max_depth": 0}
    )
    data = json.loads(result["content"][0]["text"])
    assert data["max_depth"] == 1


# ──────────────────────────────────────────────────────────────────────────
# trace_metric_impacts: capping what gets serialized
# ──────────────────────────────────────────────────────────────────────────


class _ImpactsOnly:
    """A minimal `SemanticSource` carrying influence edges and, optionally,
    identity edges declared via each metric's `decompositions`.

    `create_tools` has no `metric_impacts` parameter -- impacts reach the tool
    through `semantic_source.get_metric_impacts()`. A dense graph of hundreds
    of edges is impractical as a YAML fixture, so this stub supplies them
    directly. Every other protocol method returns empty, which is all
    `trace_metric_impacts` needs.
    """

    def __init__(
        self,
        impacts: list[MetricImpact],
        decompositions: dict[str, list[Decomposition]] | None = None,
    ) -> None:
        self._impacts = impacts
        self._decompositions = decompositions or {}
        # `create_tools` validates every impact against `get_metrics()` and
        # raises on an unknown endpoint, so the stub must declare the metrics
        # its edges name. Derived rather than passed in, so a test only ever
        # states its edges.
        names = sorted(
            {n for i in impacts for n in (i.from_metric, i.to_metric)}
            | set(self._decompositions)
        )
        self._metrics = [
            MetricDefinition(
                name=n,
                description="",
                sql_expression="x",
                decompositions=self._decompositions.get(n, []),
            )
            for n in names
        ]

    def get_metrics(self) -> list[MetricDefinition]:
        return self._metrics

    def get_metric(self, name: str) -> MetricDefinition | None:
        return next((m for m in self._metrics if m.name == name), None)

    def get_table_schema(self, schema: str, table: str) -> TableSchema | None:
        return None

    def get_table_schemas(self) -> dict[str, TableSchema]:
        return {}

    def search_metrics(self, query: str) -> list[MetricDefinition]:
        return []

    def get_relationships(self) -> list[Relationship]:
        return []

    def get_relationships_for_table(self, table: str) -> list[Relationship]:
        return []

    def get_metric_impacts(self) -> list[MetricImpact]:
        return self._impacts


async def _trace(contract: DataContract, impacts: list[MetricImpact], **args) -> dict:
    tools = create_tools(contract, semantic_source=_ImpactsOnly(impacts))
    tool = next(t for t in tools if t.name == "trace_metric_impacts")
    result = await tool.callable(args)
    return json.loads(result["content"][0]["text"])


@pytest.mark.asyncio
async def test_trace_metric_impacts_caps_the_edges_it_serializes(
    contract_no_domains: DataContract,
) -> None:
    """A dense graph must not flood the agent's context.

    The walk itself stays complete; only what this tool serializes is capped.
    """
    names = [f"m{i}" for i in range(30)]
    impacts = [
        MetricImpact(from_metric=a, to_metric=b) for a in names for b in names if a != b
    ]
    data = await _trace(
        contract_no_domains,
        impacts,
        metric_name="m0",
        direction="downstream",
        max_depth=10,
    )
    assert len(data["edges"]) == 200
    assert "870" in data["note"]  # 30 * 29 edges exist within the horizon
    assert "max_depth" in data["note"]


@pytest.mark.asyncio
async def test_trace_metric_impacts_under_the_cap_has_no_truncation_note(
    contract_no_domains: DataContract,
) -> None:
    impacts = [MetricImpact(from_metric="a", to_metric=f"b{i}") for i in range(10)]
    data = await _trace(
        contract_no_domains, impacts, metric_name="a", direction="downstream"
    )
    assert len(data["edges"]) == 10
    assert "showing" not in data.get("note", "")


@pytest.mark.asyncio
async def test_trace_metric_impacts_cap_boundary_is_exact(
    contract_no_domains: DataContract,
) -> None:
    """Pinned at 200 and 201 so an off-by-one cannot pass."""
    for total, expect_note in ((200, False), (201, True)):
        impacts = [
            MetricImpact(from_metric="hub", to_metric=f"leaf{i}") for i in range(total)
        ]
        data = await _trace(
            contract_no_domains, impacts, metric_name="hub", direction="downstream"
        )
        assert len(data["edges"]) == min(total, 200)
        assert ("showing" in data.get("note", "")) is expect_note


@pytest.mark.asyncio
async def test_truncation_keeps_identity_edges_over_influence(
    contract_no_domains: DataContract,
) -> None:
    """An identity edge is exact arithmetic; an influence edge is a
    hypothesis. When the 200-edge cap forces something to be dropped, it
    must drop the hypothesis, not the arithmetic.

    `hub` declares one `product` decomposition (2 identity edges) plus 250
    influence drivers -- 252 edges total, all one hop upstream of `hub`.
    Before the fix, influence edges were indexed first and both identity
    edges landed past the 200-edge cap and were dropped; after the fix,
    both identity edges are indexed first and survive.
    """
    decomp = Decomposition(
        operator="product",
        operands=["price", "volume"],
        convention="fold_into",
        convention_operand="price",
    )
    impacts = [
        MetricImpact(from_metric=f"driver{i}", to_metric="hub") for i in range(250)
    ]
    tools = create_tools(
        contract_no_domains,
        semantic_source=_ImpactsOnly(impacts, decompositions={"hub": [decomp]}),
    )
    tool = next(t for t in tools if t.name == "trace_metric_impacts")
    result = await tool.callable(
        {"metric_name": "hub", "direction": "upstream", "max_depth": 1}
    )
    data = json.loads(result["content"][0]["text"])
    assert len(data["edges"]) == 200
    identity_kinds = [e for e in data["edges"] if e["kind"] == "identity"]
    assert len(identity_kinds) == 2
    assert {e["from"] for e in identity_kinds} == {"price", "volume"}


@pytest.mark.asyncio
async def test_truncation_may_drop_deeper_identity_edges_and_says_so(
    contract_no_domains: DataContract,
) -> None:
    """Superseded contract for the old
    `test_truncation_keeps_identity_edges_from_deeper_nodes_too`.

    A global identity-first sort would keep `B`'s depth-2 identity edges
    ahead of `A`'s depth-1 hypotheses, but that sort was reverted (Spec
    Testing item 1): it let arithmetic connected to nothing in the payload
    outrank hypotheses that connect the payload to `start`, producing a
    disconnected result. Truncation is a straight BFS prefix now, so a
    depth-1 hub of hypotheses genuinely can fill the cap before a deeper
    node's identity edges are ever reached -- `B`'s two identity edges
    (`p -> B`, `q -> B`) are dropped here. That loss must not be silent: the
    note must name the count and point at `kinds='identity'` as the remedy.

    `hub = sum(A, B)`; `A` has 250 influence drivers (depth 2 from `hub`);
    `B = product(p, q)` (2 more identity edges, also depth 2).
    """
    hub_decomp = Decomposition(operator="sum", operands=["A", "B"])
    b_decomp = Decomposition(operator="product", operands=["p", "q"])
    impacts = [
        MetricImpact(from_metric=f"driver{i}", to_metric="A") for i in range(250)
    ]
    tools = create_tools(
        contract_no_domains,
        semantic_source=_ImpactsOnly(
            impacts, decompositions={"hub": [hub_decomp], "B": [b_decomp]}
        ),
    )
    tool = next(t for t in tools if t.name == "trace_metric_impacts")
    result = await tool.callable(
        {"metric_name": "hub", "direction": "upstream", "max_depth": 2}
    )
    data = json.loads(result["content"][0]["text"])
    assert len(data["edges"]) == 200
    identity_pairs = {
        (e["from"], e["to"]) for e in data["edges"] if e["kind"] == "identity"
    }
    # `hub`'s own 2 identity edges are depth 1, so they survive the BFS
    # prefix; `B`'s 2 identity edges are depth 2, behind 250 depth-1
    # hypotheses, so they do not.
    assert identity_pairs == {("A", "hub"), ("B", "hub")}
    assert "2 identity edge(s)" in data["note"]
    assert "kinds='identity'" in data["note"]


@pytest.mark.asyncio
async def test_truncation_never_returns_edges_disconnected_from_the_root(
    contract_no_domains: DataContract,
) -> None:
    """Spec Testing item 1 (the important one, CRITICAL finding): a global
    identity-first sort could return a payload entirely disconnected from
    the queried metric.

    `hub` has 250 influence drivers (depth 1); each `driver_i` declares
    `sum(a_i, b_i)`, contributing 2 identity edges (depth 2) -- 500 identity
    edges total. Under the reverted global sort, all 500 depth-2 identity
    edges would outrank the 250 depth-1 influence edges that connect `hub`
    to the rest of the graph, so the 200-edge cap would be filled entirely
    with identity edges naming `driver_i`/`a_i`/`b_i` metrics with no edge
    connecting any of them back to `hub` -- 200 edges, none reachable from
    `hub`. Truncation must be a BFS prefix so every edge returned is
    reachable from `hub` within the payload itself.
    """
    impacts = [
        MetricImpact(from_metric=f"driver{i}", to_metric="hub") for i in range(250)
    ]
    decompositions = {
        f"driver{i}": [Decomposition(operator="sum", operands=[f"a{i}", f"b{i}"])]
        for i in range(250)
    }
    tools = create_tools(
        contract_no_domains,
        semantic_source=_ImpactsOnly(impacts, decompositions=decompositions),
    )
    tool = next(t for t in tools if t.name == "trace_metric_impacts")
    result = await tool.callable(
        {"metric_name": "hub", "direction": "upstream", "max_depth": 2}
    )
    data = json.loads(result["content"][0]["text"])
    edges = data["edges"]
    assert len(edges) == 200

    # Walk the returned edges themselves -- not the "hub" string, not the
    # depth labels -- to find what is actually reachable from `hub`. Each
    # edge points driver -> affected ("from" -> "to"), so `hub` is reached
    # from a driver by following an edge backward: `to` already reached
    # implies `from` is now reached too. Fixpoint over the payload's own
    # edges, since a bug that mislabels depth must not be able to hide a
    # disconnected edge from this check.
    reachable = {"hub"}
    changed = True
    while changed:
        changed = False
        for edge in edges:
            if edge["to"] in reachable and edge["from"] not in reachable:
                reachable.add(edge["from"])
                changed = True
    unreachable = [
        e for e in edges if e["to"] not in reachable or e["from"] not in reachable
    ]
    assert unreachable == []


@pytest.mark.asyncio
async def test_truncation_returned_edges_are_genuinely_the_nearest(
    contract_no_domains: DataContract,
) -> None:
    """Spec Testing item 2: the note's 'showing the N nearest' and the tool
    description's 'nearest first' claims were false under the global sort --
    a graph could return depth-2 identity edges while depth-1 influence
    edges were dropped entirely. `hub` has 250 shallow (depth-1) influence
    drivers, exceeding the 200-edge cap on their own, plus deeper (depth-2)
    identity edges declared on `driver0`. With the sort reverted, the
    payload must hold only the shallowest edges the cap allows -- no depth-2
    edge may appear while a depth-1 edge existed and was left out."""
    impacts = [
        MetricImpact(from_metric=f"driver{i}", to_metric="hub") for i in range(250)
    ]
    decomp = Decomposition(operator="sum", operands=["x", "y"])
    tools = create_tools(
        contract_no_domains,
        semantic_source=_ImpactsOnly(impacts, decompositions={"driver0": [decomp]}),
    )
    tool = next(t for t in tools if t.name == "trace_metric_impacts")
    result = await tool.callable(
        {"metric_name": "hub", "direction": "upstream", "max_depth": 2}
    )
    data = json.loads(result["content"][0]["text"])
    assert len(data["edges"]) == 200
    assert {e["depth"] for e in data["edges"]} == {1}
    assert "showing the 200 nearest" in data["note"]


@pytest.mark.asyncio
async def test_kinds_identity_only_and_influence_only_survive_reordering(
    contract_no_domains: DataContract,
) -> None:
    """Indexing identity edges first must not leak them into an
    influence-only walk, nor vice versa."""
    decomp = Decomposition(operator="product", operands=["price", "volume"])
    impacts = [MetricImpact(from_metric="promo", to_metric="hub")]
    tools = create_tools(
        contract_no_domains,
        semantic_source=_ImpactsOnly(impacts, decompositions={"hub": [decomp]}),
    )
    tool = next(t for t in tools if t.name == "trace_metric_impacts")

    identity_only = json.loads(
        (
            await tool.callable(
                {"metric_name": "hub", "direction": "upstream", "kinds": "identity"}
            )
        )["content"][0]["text"]
    )
    assert {e["kind"] for e in identity_only["edges"]} == {"identity"}

    influence_only = json.loads(
        (
            await tool.callable(
                {"metric_name": "hub", "direction": "upstream", "kinds": "influence"}
            )
        )["content"][0]["text"]
    )
    assert {e["kind"] for e in influence_only["edges"]} == {"influence"}


@pytest.mark.asyncio
async def test_truncation_advice_at_the_max_depth_floor_narrows_by_kind(
    contract_no_domains: DataContract,
) -> None:
    """At max_depth=1 -- the clamp floor -- 'lower max_depth' is not
    actionable, so the note must advise narrowing by kind instead -- but only
    because this walk genuinely holds both kinds. `hub = sum(A, B)` supplies 2
    identity edges at depth 1, alongside 250 influence drivers, also at depth
    1, so narrowing to either kind actually changes the result."""
    decomp = Decomposition(operator="sum", operands=["A", "B"])
    impacts = [
        MetricImpact(from_metric=f"driver{i}", to_metric="hub") for i in range(250)
    ]
    tools = create_tools(
        contract_no_domains,
        semantic_source=_ImpactsOnly(impacts, decompositions={"hub": [decomp]}),
    )
    tool = next(t for t in tools if t.name == "trace_metric_impacts")
    result = await tool.callable(
        {"metric_name": "hub", "direction": "upstream", "max_depth": 1}
    )
    data = json.loads(result["content"][0]["text"])
    assert "Lower max_depth" not in data["note"]
    assert "kinds='identity'" in data["note"]
    assert "kinds='influence'" in data["note"]


@pytest.mark.asyncio
async def test_truncation_advice_no_kind_narrowing_when_walk_holds_one_kind(
    contract_no_domains: DataContract,
) -> None:
    """Spec Testing item 3 (single-kind case): when the full walk holds only
    influence edges, narrowing to 'influence' returns a byte-identical
    payload and narrowing to 'identity' returns nothing -- neither is useful
    advice, so the note must not suggest either, even though `kinds` was
    never narrowed by the caller. Lowering max_depth is also not actionable
    at the floor, so the note must plainly say the result is truncated with
    no narrowing available."""
    impacts = [
        MetricImpact(from_metric=f"driver{i}", to_metric="hub") for i in range(250)
    ]
    data = await _trace(
        contract_no_domains,
        impacts,
        metric_name="hub",
        direction="upstream",
        max_depth=1,
    )
    assert "kinds=" not in data["note"]
    assert "Lower max_depth" not in data["note"]
    assert "no narrowing is available" in data["note"]


@pytest.mark.asyncio
async def test_truncation_advice_above_the_floor_still_says_lower_max_depth(
    contract_no_domains: DataContract,
) -> None:
    """Lowering max_depth is only actionable when depth-1 edges alone don't
    already exceed the cap. Here `hub` has 5 direct drivers (depth 1) and
    each of those has 60 further drivers (depth 2) -- 305 edges total, but
    only 5 at depth 1 -- so re-running at max_depth=1 genuinely returns a
    complete, untruncated result."""
    impacts = [
        MetricImpact(from_metric=f"driver{i}", to_metric="hub") for i in range(5)
    ] + [
        MetricImpact(from_metric=f"subdriver{i}_{j}", to_metric=f"driver{i}")
        for i in range(5)
        for j in range(60)
    ]
    data = await _trace(
        contract_no_domains,
        impacts,
        metric_name="hub",
        direction="upstream",
        max_depth=2,
    )
    assert "Lower max_depth" in data["note"]

    lowered = await _trace(
        contract_no_domains,
        impacts,
        metric_name="hub",
        direction="upstream",
        max_depth=1,
    )
    assert "showing" not in lowered.get("note", "")
    assert len(lowered["edges"]) == 5


@pytest.mark.asyncio
async def test_truncation_advice_does_not_recommend_lowering_when_it_would_not_help(
    contract_no_domains: DataContract,
) -> None:
    """Spec Testing item 2: 250 direct (depth-1) drivers of `hub` already
    exceed the cap on their own, so re-running at max_depth=1 returns the
    identical 200-edge truncated payload -- 'Lower max_depth' must not be
    advised."""
    impacts = [
        MetricImpact(from_metric=f"driver{i}", to_metric="hub") for i in range(250)
    ]
    data = await _trace(
        contract_no_domains,
        impacts,
        metric_name="hub",
        direction="upstream",
        max_depth=2,
    )
    assert "Lower max_depth" not in data["note"]

    lowered = await _trace(
        contract_no_domains,
        impacts,
        metric_name="hub",
        direction="upstream",
        max_depth=1,
    )
    assert lowered["edges"] == data["edges"]


@pytest.mark.asyncio
async def test_truncation_advice_does_not_recommend_kind_already_narrowed(
    contract_no_domains: DataContract,
) -> None:
    """Spec Testing item 3: when `kinds` is already narrowed, re-running with
    the kind the agent already passed returns a byte-identical payload, so
    the note must not tell it to do that. It should plainly say the result is
    truncated with no narrowing available, while still keeping the true
    total and the shown count."""
    impacts = [
        MetricImpact(from_metric=f"driver{i}", to_metric="hub") for i in range(250)
    ]
    data = await _trace(
        contract_no_domains,
        impacts,
        metric_name="hub",
        direction="upstream",
        max_depth=1,
        kinds="influence",
    )
    assert "kinds=" not in data["note"]
    assert "Lower max_depth" not in data["note"]
    assert "250" in data["note"]
    assert "200" in data["note"]
    assert len(data["edges"]) == 200


@pytest.mark.asyncio
async def test_truncation_and_identity_direction_notes_join(
    contract_no_domains: DataContract,
) -> None:
    """Spec Testing item 6: truncation and identity-direction notes both
    present when both apply, joined into one `note` rather than one
    silently overwriting the other.

    `hub` has 250 influence drivers (triggers truncation) and no identity
    edges upstream of it, but is itself an operand of `parent` (triggers
    the identity-direction note, since identity edges exist downstream).
    """
    impacts = [
        MetricImpact(from_metric=f"driver{i}", to_metric="hub") for i in range(250)
    ]
    decomp = Decomposition(operator="sum", operands=["hub", "other"])
    tools = create_tools(
        contract_no_domains,
        semantic_source=_ImpactsOnly(impacts, decompositions={"parent": [decomp]}),
    )
    tool = next(t for t in tools if t.name == "trace_metric_impacts")
    result = await tool.callable(
        {"metric_name": "hub", "direction": "upstream", "max_depth": 1}
    )
    data = json.loads(result["content"][0]["text"])
    note = data["note"]
    assert "showing the 200 nearest" in note
    assert "No identity edges upstream of 'hub'" in note
