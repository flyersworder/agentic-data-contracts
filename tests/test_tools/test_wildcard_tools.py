"""Tests for tools with wildcard table resolution."""

import json

import pytest

from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.schema import (
    AllowedTable,
    DataContractSchema,
    SemanticConfig,
)
from agentic_data_contracts.tools.factory import create_tools


@pytest.fixture
def adapter() -> DuckDBAdapter:
    db = DuckDBAdapter(":memory:")
    db.connection.execute("""
        CREATE SCHEMA IF NOT EXISTS analytics;
        CREATE TABLE analytics.orders (id INTEGER, amount DECIMAL);
        CREATE TABLE analytics.customers (id INTEGER, name VARCHAR);
        INSERT INTO analytics.orders VALUES (1, 100.00);
    """)
    return db


@pytest.fixture
def wildcard_contract() -> DataContract:
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate({"schema": "analytics", "tables": ["*"]}),
            ],
        ),
    )
    return DataContract(schema)


@pytest.mark.asyncio
async def test_run_query_with_wildcard_tables(
    wildcard_contract: DataContract, adapter: DuckDBAdapter
) -> None:
    tools = create_tools(wildcard_contract, adapter=adapter)
    tool = next(t for t in tools if t.name == "run_query")
    result = await tool.callable({"sql": "SELECT id, amount FROM analytics.orders"})
    text = result["content"][0]["text"]
    assert "100" in text


@pytest.mark.asyncio
async def test_inspect_query_with_wildcard_tables(
    wildcard_contract: DataContract, adapter: DuckDBAdapter
) -> None:
    tools = create_tools(wildcard_contract, adapter=adapter)
    tool = next(t for t in tools if t.name == "inspect_query")
    # analytics.orders should be allowed after wildcard resolution
    result = await tool.callable({"sql": "SELECT id FROM analytics.orders"})
    data = json.loads(result["content"][0]["text"])
    assert data["valid"] is True
