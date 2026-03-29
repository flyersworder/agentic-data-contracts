"""Tests for list_tables pagination."""

import json

import pytest

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.schema import (
    AllowedTable,
    DataContractSchema,
    SemanticConfig,
)
from agentic_data_contracts.tools.factory import create_tools


@pytest.fixture
def large_contract() -> DataContract:
    """Contract with many tables to test pagination."""
    tables = [f"table_{i}" for i in range(60)]
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate({"schema": "analytics", "tables": tables}),
            ],
        ),
    )
    return DataContract(schema)


@pytest.mark.asyncio
async def test_list_tables_default_limit(
    large_contract: DataContract,
) -> None:
    tools = create_tools(large_contract)
    tool = next(t for t in tools if t.name == "list_tables")
    result = await tool.callable({})
    data = json.loads(result["content"][0]["text"])
    assert len(data["tables"]) == 50  # default limit
    assert data["total"] == 60
    assert data["next_offset"] == 50


@pytest.mark.asyncio
async def test_list_tables_custom_limit(
    large_contract: DataContract,
) -> None:
    tools = create_tools(large_contract)
    tool = next(t for t in tools if t.name == "list_tables")
    result = await tool.callable({"limit": 10})
    data = json.loads(result["content"][0]["text"])
    assert len(data["tables"]) == 10
    assert data["total"] == 60
    assert data["next_offset"] == 10


@pytest.mark.asyncio
async def test_list_tables_with_offset(
    large_contract: DataContract,
) -> None:
    tools = create_tools(large_contract)
    tool = next(t for t in tools if t.name == "list_tables")
    result = await tool.callable({"limit": 10, "offset": 50})
    data = json.loads(result["content"][0]["text"])
    assert len(data["tables"]) == 10
    assert data["total"] == 60
    assert "next_offset" not in data  # last page


@pytest.mark.asyncio
async def test_list_tables_small_set_no_next(
    fixtures_dir,
) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "minimal_contract.yml")
    tools = create_tools(dc)
    tool = next(t for t in tools if t.name == "list_tables")
    result = await tool.callable({})
    data = json.loads(result["content"][0]["text"])
    assert data["total"] == 1
    assert "next_offset" not in data
