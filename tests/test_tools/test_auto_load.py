"""Tests for create_tools auto-loading semantic source from contract config."""

from pathlib import Path

import pytest

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.schema import (
    AllowedTable,
    DataContractSchema,
    SemanticConfig,
)
from agentic_data_contracts.core.schema import (
    SemanticSource as SemanticSourceConfig,
)
from agentic_data_contracts.tools.factory import create_tools


@pytest.fixture
def contract_with_yaml_source(fixtures_dir: Path) -> DataContract:
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            source=SemanticSourceConfig(
                type="yaml",
                path=str(fixtures_dir / "semantic_source.yml"),
            ),
            allowed_tables=[
                AllowedTable.model_validate(
                    {"schema": "analytics", "tables": ["orders"]}
                ),
            ],
        ),
    )
    return DataContract(schema)


@pytest.mark.asyncio
async def test_create_tools_auto_loads_semantic(
    contract_with_yaml_source: DataContract,
) -> None:
    """create_tools should auto-load semantic source when not explicitly passed."""
    tools = create_tools(contract_with_yaml_source)
    tool = next(t for t in tools if t.name == "list_metrics")
    result = await tool.callable({})
    text = result["content"][0]["text"]
    assert "total_revenue" in text


@pytest.mark.asyncio
async def test_create_tools_auto_loads_lookup(
    contract_with_yaml_source: DataContract,
) -> None:
    tools = create_tools(contract_with_yaml_source)
    tool = next(t for t in tools if t.name == "lookup_metric")
    result = await tool.callable({"metric_name": "total_revenue"})
    text = result["content"][0]["text"]
    assert "SUM(amount)" in text


@pytest.mark.asyncio
async def test_create_tools_no_source_configured() -> None:
    """When no source configured, metric tools should say no source."""
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(),
    )
    dc = DataContract(schema)
    tools = create_tools(dc)
    tool = next(t for t in tools if t.name == "list_metrics")
    result = await tool.callable({})
    text = result["content"][0]["text"]
    assert "no semantic source" in text.lower()
