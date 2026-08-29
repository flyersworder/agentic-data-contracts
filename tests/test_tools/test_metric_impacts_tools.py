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
    """A minimal `SemanticSource` carrying nothing but influence edges.

    `create_tools` has no `metric_impacts` parameter -- impacts reach the tool
    through `semantic_source.get_metric_impacts()`. A dense graph of hundreds
    of edges is impractical as a YAML fixture, so this stub supplies them
    directly. Every other protocol method returns empty, which is all
    `trace_metric_impacts` needs.
    """

    def __init__(self, impacts: list[MetricImpact]) -> None:
        self._impacts = impacts
        # `create_tools` validates every impact against `get_metrics()` and
        # raises on an unknown endpoint, so the stub must declare the metrics
        # its edges name. Derived rather than passed in, so a test only ever
        # states its edges.
        names = sorted({n for i in impacts for n in (i.from_metric, i.to_metric)})
        self._metrics = [
            MetricDefinition(name=n, description="", sql_expression="x") for n in names
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
