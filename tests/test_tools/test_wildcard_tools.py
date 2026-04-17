"""Tests for tools with wildcard table resolution."""

import json
import logging

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


def test_create_tools_warns_when_wildcard_without_adapter(
    wildcard_contract: DataContract, caplog: pytest.LogCaptureFixture
) -> None:
    """Wildcard tables without an adapter leave tables unresolved silently —
    emit a warning to flag the misconfiguration for developers."""
    with caplog.at_level(logging.WARNING):
        create_tools(wildcard_contract, adapter=None)
    assert any(
        "wildcard" in msg.lower() and "adapter" in msg.lower()
        for msg in caplog.messages
    )


def test_create_tools_no_warning_when_wildcard_resolved(
    wildcard_contract: DataContract,
    adapter: DuckDBAdapter,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """When an adapter is available, wildcard resolution runs silently."""
    with caplog.at_level(logging.WARNING):
        create_tools(wildcard_contract, adapter=adapter)
    assert not any(
        "wildcard" in msg.lower() and "adapter" in msg.lower()
        for msg in caplog.messages
    )
