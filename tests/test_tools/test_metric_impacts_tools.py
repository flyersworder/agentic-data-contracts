"""Tests for metric-impact enrichment in list_metrics/lookup_metric and the
new trace_metric_impacts tool, plus back-compat for Domain.metrics reverse lookup."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.schema import (
    AllowedTable,
    DataContractSchema,
    Domain,
    SemanticConfig,
)
from agentic_data_contracts.semantic.yaml_source import YamlSource
from agentic_data_contracts.tools.factory import create_tools


@pytest.fixture
def semantic(fixtures_dir: Path) -> YamlSource:
    return YamlSource(fixtures_dir / "semantic_source.yml")


@pytest.fixture
def contract_no_domains(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml(fixtures_dir / "valid_contract.yml")


@pytest.fixture
def contract_with_domains() -> DataContract:
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate(
                    {"schema": "analytics", "tables": ["orders"]}
                ),
            ],
            domains=[
                Domain(
                    name="revenue",
                    summary="Financial metrics",
                    description="Revenue domain.",
                    # Domain-first declaration: total_revenue lives here too.
                    metrics=["total_revenue"],
                ),
            ],
        ),
    )
    return DataContract(schema)


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
# Back-compat: Domain.metrics reverse lookup
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_effective_domains_merges_contract_domain_metrics(
    contract_with_domains: DataContract, semantic: YamlSource
) -> None:
    """Domain.metrics=['total_revenue'] should appear in the metric's domains
    even if (hypothetically) the metric hadn't self-declared the revenue domain."""
    tools = create_tools(contract_with_domains, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "lookup_metric")
    result = await tool.callable({"metric_name": "total_revenue"})
    data = json.loads(result["content"][0]["text"])
    # "revenue" appears from both metric.domains AND Domain.metrics, but the
    # effective list should de-duplicate.
    assert data["domains"].count("revenue") == 1


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
