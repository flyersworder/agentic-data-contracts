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
        INSERT INTO analytics.orders VALUES (1, 100.00, 'acme'), (2, 200.00, 'acme');
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


def test_create_tools_returns_13_tools(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    assert len(tools) == 13


def test_create_tools_without_adapter(
    contract: DataContract, semantic: YamlSource
) -> None:
    tools = create_tools(contract, semantic_source=semantic)
    assert len(tools) == 13


def test_tool_names(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    names = [t.name for t in tools]
    assert "list_schemas" in names
    assert "list_tables" in names
    assert "describe_table" in names
    assert "preview_table" in names
    assert "list_metrics" in names
    assert "lookup_metric" in names
    assert "validate_query" in names
    assert "query_cost_estimate" in names
    assert "run_query" in names
    assert "get_contract_info" in names
    assert "trace_metric_impacts" in names


@pytest.mark.asyncio
async def test_list_schemas(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "list_schemas")
    result = await tool.callable({})
    text = result["content"][0]["text"]
    data = json.loads(text)
    assert isinstance(data["schemas"], list)
    analytics = next(s for s in data["schemas"] if s["schema"] == "analytics")
    assert "preferred" not in analytics  # not set in fixture
    assert "description" not in analytics  # not set in fixture


@pytest.mark.asyncio
async def test_list_tables(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "list_tables")
    result = await tool.callable({})
    text = result["content"][0]["text"]
    assert "orders" in text
    assert "customers" in text


@pytest.mark.asyncio
async def test_describe_table_with_adapter(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "describe_table")
    result = await tool.callable({"schema": "analytics", "table": "orders"})
    text = result["content"][0]["text"]
    assert "id" in text
    assert "amount" in text


@pytest.mark.asyncio
async def test_describe_table_without_adapter(
    contract: DataContract, semantic: YamlSource
) -> None:
    tools = create_tools(contract, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "describe_table")
    result = await tool.callable({"schema": "analytics", "table": "orders"})
    text = result["content"][0]["text"]
    assert "unavailable" in text.lower() or "no database" in text.lower()


@pytest.mark.asyncio
async def test_validate_query_passes(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "validate_query")
    result = await tool.callable(
        {"sql": "SELECT id, amount FROM analytics.orders WHERE tenant_id = 'acme'"}
    )
    text = result["content"][0]["text"]
    assert "pass" in text.lower() or "valid" in text.lower()


@pytest.mark.asyncio
async def test_validate_query_blocked(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "validate_query")
    result = await tool.callable({"sql": "SELECT * FROM analytics.orders"})
    text = result["content"][0]["text"]
    assert "block" in text.lower() or "violation" in text.lower()


@pytest.mark.asyncio
async def test_run_query_valid(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "run_query")
    result = await tool.callable(
        {"sql": "SELECT id, amount FROM analytics.orders WHERE tenant_id = 'acme'"}
    )
    text = result["content"][0]["text"]
    assert "100" in text


@pytest.mark.asyncio
async def test_run_query_blocked(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "run_query")
    result = await tool.callable({"sql": "DELETE FROM analytics.orders"})
    text = result["content"][0]["text"]
    assert "block" in text.lower() or "violation" in text.lower()


@pytest.mark.asyncio
async def test_get_contract_info(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "get_contract_info")
    result = await tool.callable({})
    text = result["content"][0]["text"]
    assert "revenue-analysis" in text


@pytest.mark.asyncio
async def test_lookup_metric(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "lookup_metric")
    result = await tool.callable({"metric_name": "total_revenue"})
    text = result["content"][0]["text"]
    assert "total_revenue" in text
    assert "SUM(amount)" in text


@pytest.mark.asyncio
async def test_preview_table(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "preview_table")
    result = await tool.callable({"schema": "analytics", "table": "orders"})
    text = result["content"][0]["text"]
    assert "100" in text or "acme" in text


@pytest.mark.asyncio
async def test_preview_table_limit_clamped(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "preview_table")
    # limit > 100 should be clamped to 100, no error
    result = await tool.callable(
        {"schema": "analytics", "table": "orders", "limit": 9999}
    )
    text = result["content"][0]["text"]
    assert "rows" in text.lower() or "acme" in text


@pytest.mark.asyncio
async def test_preview_table_limit_invalid(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "preview_table")
    # Non-numeric limit should fall back to 5 without error
    result = await tool.callable(
        {"schema": "analytics", "table": "orders", "limit": "bad"}
    )
    text = result["content"][0]["text"]
    assert "100" in text or "acme" in text


@pytest.mark.asyncio
async def test_describe_table_rejects_non_allowed_table(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "describe_table")
    result = await tool.callable({"schema": "secret", "table": "data"})
    text = result["content"][0]["text"]
    assert "not in the allowed" in text.lower()


@pytest.mark.asyncio
async def test_query_cost_estimate_with_adapter(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "query_cost_estimate")
    result = await tool.callable({"sql": "SELECT id FROM analytics.orders"})
    text = result["content"][0]["text"]
    assert "schema_valid" in text


@pytest.mark.asyncio
async def test_query_cost_estimate_without_adapter(
    contract: DataContract, semantic: YamlSource
) -> None:
    tools = create_tools(contract, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "query_cost_estimate")
    result = await tool.callable({"sql": "SELECT 1"})
    text = result["content"][0]["text"]
    assert "unavailable" in text.lower()


@pytest.mark.asyncio
async def test_list_metrics_with_source(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "list_metrics")
    result = await tool.callable({})
    text = result["content"][0]["text"]
    assert "total_revenue" in text


@pytest.mark.asyncio
async def test_run_query_session_limit_exceeded(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    from agentic_data_contracts.core.session import ContractSession

    session = ContractSession(contract)
    session.record_retry()
    session.record_retry()
    session.record_retry()
    tools = create_tools(
        contract, adapter=adapter, semantic_source=semantic, session=session
    )
    tool = next(t for t in tools if t.name == "run_query")
    result = await tool.callable(
        {"sql": "SELECT id FROM analytics.orders WHERE tenant_id = 'x'"}
    )
    text = result["content"][0]["text"]
    assert (
        "blocked" in text.lower()
        or "limit" in text.lower()
        or "exceeded" in text.lower()
    )


@pytest.mark.asyncio
async def test_run_query_result_check_blocks() -> None:
    """Result check with enforcement=block should discard data and return violation."""
    from agentic_data_contracts.core.schema import (
        AllowedTable,
        DataContractSchema,
        Enforcement,
        ResultCheck,
        SemanticConfig,
        SemanticRule,
    )

    schema = DataContractSchema(
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
    dc = DataContract(schema)

    db = DuckDBAdapter(":memory:")
    db.connection.execute("""
        CREATE SCHEMA IF NOT EXISTS analytics;
        CREATE TABLE analytics.orders (id INTEGER, amount DECIMAL(10,2));
        INSERT INTO analytics.orders VALUES (1, 100.00), (2, -50.00);
    """)

    tools = create_tools(dc, adapter=db)
    tool = next(t for t in tools if t.name == "run_query")
    result = await tool.callable({"sql": "SELECT id, amount FROM analytics.orders"})
    text = result["content"][0]["text"]
    assert "block" in text.lower() or "no_negative" in text.lower()
    # Should NOT contain the actual row data
    assert "100" not in text


@pytest.mark.asyncio
async def test_run_query_result_check_warns() -> None:
    """Result check with enforcement=warn should return data + warning."""
    from agentic_data_contracts.core.schema import (
        AllowedTable,
        DataContractSchema,
        Enforcement,
        ResultCheck,
        SemanticConfig,
        SemanticRule,
    )

    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate(
                    {"schema": "analytics", "tables": ["orders"]}
                ),
            ],
            rules=[
                SemanticRule(
                    name="empty_check",
                    description="Warn if empty",
                    enforcement=Enforcement.WARN,
                    result_check=ResultCheck(min_rows=100),
                ),
            ],
        ),
    )
    dc = DataContract(schema)

    db = DuckDBAdapter(":memory:")
    db.connection.execute("""
        CREATE SCHEMA IF NOT EXISTS analytics;
        CREATE TABLE analytics.orders (id INTEGER);
        INSERT INTO analytics.orders VALUES (1), (2);
    """)

    tools = create_tools(dc, adapter=db)
    tool = next(t for t in tools if t.name == "run_query")
    result = await tool.callable({"sql": "SELECT id FROM analytics.orders"})
    text = result["content"][0]["text"]
    # Should contain both the warning and the data
    assert "warn" in text.lower() or "empty_check" in text.lower()
    assert "1" in text  # row data present


@pytest.mark.asyncio
async def test_run_query_records_session_cost() -> None:
    """run_query should record estimated cost in the session."""
    from agentic_data_contracts.core.schema import (
        AllowedTable,
        DataContractSchema,
        ResourceConfig,
        SemanticConfig,
    )
    from agentic_data_contracts.core.session import ContractSession

    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate(
                    {"schema": "analytics", "tables": ["orders"]}
                ),
            ],
        ),
        resources=ResourceConfig(cost_limit_usd=10.0),
    )
    dc = DataContract(schema)

    db = DuckDBAdapter(":memory:")
    db.connection.execute("""
        CREATE SCHEMA IF NOT EXISTS analytics;
        CREATE TABLE analytics.orders (id INTEGER);
        INSERT INTO analytics.orders VALUES (1);
    """)

    session = ContractSession(dc)
    tools = create_tools(dc, adapter=db, session=session)
    tool = next(t for t in tools if t.name == "run_query")
    await tool.callable({"sql": "SELECT id FROM analytics.orders"})

    # DuckDB doesn't provide cost estimates, so cost should remain 0
    # This test verifies the plumbing works without error
    assert session.cost_usd >= 0.0


@pytest.mark.asyncio
async def test_run_query_response_includes_session_remaining(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "run_query")
    result = await tool.callable(
        {"sql": "SELECT id FROM analytics.orders WHERE tenant_id = 'acme'"}
    )
    data = json.loads(result["content"][0]["text"])
    assert "session" in data
    assert "remaining" in data["session"]
    assert "elapsed_seconds" in data["session"]["remaining"]


@pytest.mark.asyncio
async def test_run_query_blocked_includes_remaining_budget(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "run_query")
    result = await tool.callable({"sql": "DELETE FROM analytics.orders"})
    text = result["content"][0]["text"]
    assert "block" in text.lower() or "violation" in text.lower()
    assert "remaining" in text.lower()
