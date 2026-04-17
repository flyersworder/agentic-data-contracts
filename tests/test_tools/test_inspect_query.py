"""Tests for the inspect_query tool (merge of validate_query + query_cost_estimate)."""

import json
from pathlib import Path

import pytest

from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.semantic.yaml_source import YamlSource
from agentic_data_contracts.tools.factory import create_tools


@pytest.fixture
def contract(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml(fixtures_dir / "valid_contract.yml")


@pytest.fixture
def adapter() -> DuckDBAdapter:
    db = DuckDBAdapter(":memory:")
    db.connection.execute(
        """
        CREATE SCHEMA IF NOT EXISTS analytics;
        CREATE TABLE analytics.orders (
            id INTEGER, amount DECIMAL(10,2), tenant_id VARCHAR
        );
        INSERT INTO analytics.orders VALUES (1, 100.00, 'acme');
        CREATE TABLE analytics.customers (id INTEGER, name VARCHAR, tenant_id VARCHAR);
        CREATE TABLE analytics.subscriptions (
            id INTEGER, plan VARCHAR, tenant_id VARCHAR
        );
        """
    )
    return db


@pytest.fixture
def semantic(fixtures_dir: Path) -> YamlSource:
    return YamlSource(fixtures_dir / "semantic_source.yml")


@pytest.mark.asyncio
async def test_inspect_query_valid_with_adapter(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "inspect_query")
    result = await tool.callable(
        {"sql": "SELECT id, amount FROM analytics.orders WHERE tenant_id = 'acme'"}
    )
    data = json.loads(result["content"][0]["text"])
    assert data["valid"] is True
    assert data["violations"] == []
    assert data["schema_valid"] is True


@pytest.mark.asyncio
async def test_inspect_query_blocked_surfaces_violations(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "inspect_query")
    result = await tool.callable({"sql": "SELECT * FROM analytics.orders"})
    data = json.loads(result["content"][0]["text"])
    assert data["valid"] is False
    assert len(data["violations"]) >= 1


@pytest.mark.asyncio
async def test_inspect_query_no_adapter(
    contract: DataContract, semantic: YamlSource
) -> None:
    tools = create_tools(contract, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "inspect_query")
    result = await tool.callable(
        {"sql": "SELECT id FROM analytics.orders WHERE tenant_id = 'acme'"}
    )
    data = json.loads(result["content"][0]["text"])
    # Layer 1 still runs; EXPLAIN fields are absent or null
    assert "valid" in data
    assert data.get("estimated_cost_usd") is None
    assert data.get("estimated_rows") is None


@pytest.mark.asyncio
async def test_inspect_query_returns_pending_result_checks(
    adapter: DuckDBAdapter,
) -> None:
    from agentic_data_contracts.core.schema import (
        AllowedTable,
        DataContractSchema,
        Enforcement,
        ResultCheck,
        SemanticConfig,
        SemanticRule,
    )

    dc = DataContract(
        DataContractSchema(
            name="test",
            semantic=SemanticConfig(
                allowed_tables=[
                    AllowedTable.model_validate(
                        {"schema": "analytics", "tables": ["orders"]}
                    ),
                ],
                rules=[
                    SemanticRule(
                        name="no_negative",
                        description="No negative amounts",
                        enforcement=Enforcement.BLOCK,
                        result_check=ResultCheck(column="amount", min_value=0),
                    ),
                ],
            ),
        )
    )
    tools = create_tools(dc, adapter=adapter)
    tool = next(t for t in tools if t.name == "inspect_query")
    result = await tool.callable({"sql": "SELECT id, amount FROM analytics.orders"})
    data = json.loads(result["content"][0]["text"])
    assert "no_negative" in data["pending_result_checks"]
