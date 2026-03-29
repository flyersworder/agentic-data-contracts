"""Tests for Claude Agent SDK MCP server integration."""

from pathlib import Path

import pytest

from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.schema import (
    AllowedTable,
    DataContractSchema,
    SemanticConfig,
)
from agentic_data_contracts.tools.factory import create_tools
from agentic_data_contracts.tools.sdk import create_sdk_mcp_server


@pytest.fixture
def contract_no_source() -> DataContract:
    """Contract without a semantic source — avoids file path issues."""
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate(
                    {"schema": "analytics", "tables": ["orders"]}
                ),
            ],
        ),
    )
    return DataContract(schema)


@pytest.fixture
def contract_with_source(fixtures_dir: Path) -> DataContract:
    """Contract with semantic source pointing to real fixture."""
    from agentic_data_contracts.core.schema import (
        SemanticSource as SemanticSourceConfig,
    )

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
        """
    )
    return db


def test_create_sdk_mcp_server_returns_config(
    contract_no_source: DataContract, adapter: DuckDBAdapter
) -> None:
    server = create_sdk_mcp_server(contract_no_source, adapter=adapter)
    assert server is not None


def test_create_sdk_mcp_server_with_prebuilt_tools(
    contract_no_source: DataContract, adapter: DuckDBAdapter
) -> None:
    tools = create_tools(contract_no_source, adapter=adapter)
    server = create_sdk_mcp_server(contract_no_source, tools=tools)
    assert server is not None


def test_create_sdk_mcp_server_with_semantic_source(
    contract_with_source: DataContract, adapter: DuckDBAdapter
) -> None:
    server = create_sdk_mcp_server(contract_with_source, adapter=adapter)
    assert server is not None


def test_create_sdk_mcp_server_auto_creates_tools(
    contract_no_source: DataContract,
) -> None:
    server = create_sdk_mcp_server(contract_no_source)
    assert server is not None


def test_create_sdk_mcp_server_custom_name(
    contract_no_source: DataContract,
) -> None:
    server = create_sdk_mcp_server(
        contract_no_source,
        server_name="my-server",
        server_version="2.0.0",
    )
    assert server is not None


def test_top_level_import() -> None:
    from agentic_data_contracts import create_sdk_mcp_server as fn

    assert fn is not None
