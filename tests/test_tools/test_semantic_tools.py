"""Tests for enhanced lookup_metric (fuzzy) and list_metrics (domain filter)."""

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
def contract_with_domains(fixtures_dir: Path) -> DataContract:
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
                ),
                Domain(
                    name="engagement",
                    summary="Customer activity",
                    description="Engagement domain.",
                ),
            ],
        ),
    )
    return DataContract(schema)


@pytest.fixture
def contract_no_domains(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml(fixtures_dir / "valid_contract.yml")


@pytest.fixture
def contract_revenue_only() -> DataContract:
    """Catalog lists only `revenue`. The shared semantic_source.yml has a metric
    (active_customers) that also declares `engagement` — uncataloged here."""
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
                ),
            ],
        ),
    )
    return DataContract(schema)


@pytest.mark.asyncio
async def test_list_metrics_uncataloged_domain_not_found(
    contract_revenue_only: DataContract, semantic: YamlSource
) -> None:
    """Catalog is authoritative: a domain a metric declares but the contract does
    not catalog is not navigable — list_metrics agrees with lookup_domain."""
    tools = create_tools(contract_revenue_only, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "list_metrics")

    result = await tool.callable({"domain": "engagement"})  # declared, not cataloged
    assert "not found" in result["content"][0]["text"].lower()

    # revenue IS cataloged → filter works
    result2 = await tool.callable({"domain": "revenue"})
    data2 = json.loads(result2["content"][0]["text"])
    names = sorted(m["name"] for m in data2["metrics"])
    assert names == ["active_customers", "total_revenue"]


def test_metric_referencing_uncataloged_domain_warns(
    contract_revenue_only: DataContract,
    fixtures_dir: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Metric-first mirror of the old 'unknown metric' check: warn when a metric
    self-declares a domain absent from the contract's catalog (typo/rename)."""
    source = YamlSource(fixtures_dir / "semantic_source.yml")
    with caplog.at_level(logging.WARNING):
        create_tools(contract_revenue_only, semantic_source=source)
    assert any("engagement" in msg for msg in caplog.messages)


@pytest.mark.asyncio
async def test_lookup_metric_exact_match(
    contract_no_domains: DataContract, semantic: YamlSource
) -> None:
    tools = create_tools(contract_no_domains, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "lookup_metric")
    result = await tool.callable({"metric_name": "total_revenue"})
    text = result["content"][0]["text"]
    data = json.loads(text)
    assert data["name"] == "total_revenue"
    assert "SUM(amount)" in data["sql_expression"]


@pytest.mark.asyncio
async def test_lookup_metric_fuzzy_fallback(
    contract_no_domains: DataContract, semantic: YamlSource
) -> None:
    tools = create_tools(contract_no_domains, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "lookup_metric")
    result = await tool.callable({"metric_name": "revenue from orders"})
    text = result["content"][0]["text"]
    data = json.loads(text)
    assert data["exact_match"] is False
    assert len(data["candidates"]) >= 1
    names = [c["name"] for c in data["candidates"]]
    assert "total_revenue" in names


@pytest.mark.asyncio
async def test_lookup_metric_no_match(
    contract_no_domains: DataContract, semantic: YamlSource
) -> None:
    tools = create_tools(contract_no_domains, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "lookup_metric")
    result = await tool.callable({"metric_name": "xyznonexistent"})
    text = result["content"][0]["text"]
    assert "not found" in text.lower()


@pytest.mark.asyncio
async def test_list_metrics_no_domain(
    contract_no_domains: DataContract, semantic: YamlSource
) -> None:
    tools = create_tools(contract_no_domains, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "list_metrics")
    result = await tool.callable({})
    text = result["content"][0]["text"]
    data = json.loads(text)
    assert len(data["metrics"]) == 2


@pytest.mark.asyncio
async def test_list_metrics_with_domain(
    contract_with_domains: DataContract, semantic: YamlSource
) -> None:
    tools = create_tools(contract_with_domains, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "list_metrics")
    result = await tool.callable({"domain": "revenue"})
    text = result["content"][0]["text"]
    data = json.loads(text)
    # Metric-first membership: both metrics self-declare the "revenue" domain
    # (total_revenue -> [revenue]; active_customers -> [engagement, revenue]).
    names = sorted(m["name"] for m in data["metrics"])
    assert names == ["active_customers", "total_revenue"]


@pytest.mark.asyncio
async def test_list_metrics_unknown_domain(
    contract_with_domains: DataContract, semantic: YamlSource
) -> None:
    tools = create_tools(contract_with_domains, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "list_metrics")
    result = await tool.callable({"domain": "nonexistent"})
    text = result["content"][0]["text"]
    assert "not found" in text.lower()


@pytest.mark.asyncio
async def test_lookup_domain_exact_match(
    contract_with_domains: DataContract, semantic: YamlSource
) -> None:
    tools = create_tools(contract_with_domains, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "lookup_domain")
    result = await tool.callable({"name": "revenue"})
    text = result["content"][0]["text"]
    data = json.loads(text)
    assert data["name"] == "revenue"
    assert "summary" in data
    assert "description" in data
    # Members are reverse-looked-up from metric.domains: total_revenue and
    # active_customers both declare the "revenue" domain.
    names = [m["name"] for m in data["metrics"]]
    assert names == ["total_revenue", "active_customers"]
    assert all(m["description"] != "" for m in data["metrics"])  # enriched


@pytest.mark.asyncio
async def test_lookup_domain_not_found(
    contract_with_domains: DataContract, semantic: YamlSource
) -> None:
    tools = create_tools(contract_with_domains, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "lookup_domain")
    result = await tool.callable({"name": "xyznonexistent"})
    text = result["content"][0]["text"]
    assert "not found" in text.lower()


@pytest.mark.asyncio
async def test_lookup_domain_fuzzy_match(
    contract_with_domains: DataContract, semantic: YamlSource
) -> None:
    tools = create_tools(contract_with_domains, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "lookup_domain")
    result = await tool.callable({"name": "rev"})
    text = result["content"][0]["text"]
    data = json.loads(text)
    assert data["exact_match"] is False
    assert len(data["candidates"]) >= 1
    assert data["candidates"][0]["name"] == "revenue"


@pytest.mark.asyncio
async def test_lookup_domain_no_semantic_source(
    contract_with_domains: DataContract,
) -> None:
    tools = create_tools(contract_with_domains, semantic_source=None)
    tool = next(t for t in tools if t.name == "lookup_domain")
    result = await tool.callable({"name": "revenue"})
    text = result["content"][0]["text"]
    data = json.loads(text)
    assert data["name"] == "revenue"
    # Membership lives on metrics (in the semantic source); without a source
    # there is nothing to reverse-look-up, so the member list is empty.
    assert data["metrics"] == []


@pytest.mark.asyncio
async def test_domain_validation_warns_unknown_table(
    fixtures_dir: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
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
                    tables=["analytics.orders", "analytics.nonexistent"],
                ),
            ],
        ),
    )
    dc = DataContract(schema)
    source = YamlSource(fixtures_dir / "semantic_source.yml")

    with caplog.at_level(logging.WARNING):
        create_tools(dc, semantic_source=source)

    assert any("analytics.nonexistent" in msg for msg in caplog.messages)
