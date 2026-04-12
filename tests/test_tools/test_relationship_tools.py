"""Tests for the lookup_relationships tool."""

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


@pytest.fixture
def multi_rel_source(tmp_path: Path) -> YamlSource:
    yml = """\
relationships:
  - from: s.orders.customer_id
    to: s.customers.id
    type: many_to_one
    description: "Order belongs to customer"
    required_filter: "status != 'cancelled'"
  - from: s.orders.product_id
    to: s.products.id
    type: many_to_one
    description: "Order contains product"
  - from: s.reviews.customer_id
    to: s.customers.id
    type: many_to_one
    description: "Review written by customer"
  - from: s.customers.region_id
    to: s.regions.id
    type: many_to_one
    description: "Customer in region"
"""
    (tmp_path / "rels.yml").write_text(yml)
    return YamlSource(tmp_path / "rels.yml")


@pytest.fixture
def contract() -> DataContract:
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate(
                    {
                        "schema": "s",
                        "tables": [
                            "orders",
                            "customers",
                            "products",
                            "reviews",
                            "regions",
                        ],
                    }
                ),
            ],
        ),
    )
    return DataContract(schema)


@pytest.mark.asyncio
async def test_lookup_relationships_by_table(
    contract: DataContract, multi_rel_source: YamlSource
) -> None:
    tools = create_tools(contract, semantic_source=multi_rel_source)
    tool = next(t for t in tools if t.name == "lookup_relationships")
    result = await tool.callable({"table": "s.orders"})
    data = json.loads(result["content"][0]["text"])
    assert len(data["relationships"]) == 2
    froms = {r["from"] for r in data["relationships"]}
    assert "s.orders.customer_id" in froms
    assert "s.orders.product_id" in froms


@pytest.mark.asyncio
async def test_lookup_relationships_hub_table(
    contract: DataContract, multi_rel_source: YamlSource
) -> None:
    """customers is target of orders and reviews, plus has FK to regions."""
    tools = create_tools(contract, semantic_source=multi_rel_source)
    tool = next(t for t in tools if t.name == "lookup_relationships")
    result = await tool.callable({"table": "s.customers"})
    data = json.loads(result["content"][0]["text"])
    assert len(data["relationships"]) == 3


@pytest.mark.asyncio
async def test_lookup_relationships_with_target_direct(
    contract: DataContract, multi_rel_source: YamlSource
) -> None:
    tools = create_tools(contract, semantic_source=multi_rel_source)
    tool = next(t for t in tools if t.name == "lookup_relationships")
    result = await tool.callable({"table": "s.orders", "target_table": "s.customers"})
    data = json.loads(result["content"][0]["text"])
    assert len(data["join_path"]) == 1
    assert data["join_path"][0]["from"] == "s.orders.customer_id"
    assert data["join_path"][0]["required_filter"] == "status != 'cancelled'"


@pytest.mark.asyncio
async def test_lookup_relationships_with_target_multi_hop(
    contract: DataContract, multi_rel_source: YamlSource
) -> None:
    """orders -> customers -> regions requires 2 hops."""
    tools = create_tools(contract, semantic_source=multi_rel_source)
    tool = next(t for t in tools if t.name == "lookup_relationships")
    result = await tool.callable({"table": "s.orders", "target_table": "s.regions"})
    data = json.loads(result["content"][0]["text"])
    assert len(data["join_path"]) == 2


@pytest.mark.asyncio
async def test_lookup_relationships_no_match(
    contract: DataContract, multi_rel_source: YamlSource
) -> None:
    tools = create_tools(contract, semantic_source=multi_rel_source)
    tool = next(t for t in tools if t.name == "lookup_relationships")
    result = await tool.callable({"table": "s.nonexistent"})
    text = result["content"][0]["text"]
    assert "no relationships" in text.lower()


@pytest.mark.asyncio
async def test_lookup_relationships_no_path(
    contract: DataContract, multi_rel_source: YamlSource
) -> None:
    tools = create_tools(contract, semantic_source=multi_rel_source)
    tool = next(t for t in tools if t.name == "lookup_relationships")
    result = await tool.callable({"table": "s.orders", "target_table": "s.nonexistent"})
    text = result["content"][0]["text"]
    assert "no join path" in text.lower()


@pytest.mark.asyncio
async def test_lookup_relationships_no_semantic_source(
    contract: DataContract,
) -> None:
    tools = create_tools(contract, semantic_source=None)
    tool = next(t for t in tools if t.name == "lookup_relationships")
    result = await tool.callable({"table": "s.orders"})
    text = result["content"][0]["text"]
    assert "no semantic source" in text.lower()
