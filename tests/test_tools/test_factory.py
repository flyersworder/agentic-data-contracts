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


def test_create_tools_returns_9_tools(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    assert len(tools) == 9


def test_create_tools_without_adapter(
    contract: DataContract, semantic: YamlSource
) -> None:
    tools = create_tools(contract, semantic_source=semantic)
    assert len(tools) == 9


def test_tool_names(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    names = {t.name for t in tools}
    expected = {
        "describe_table",
        "preview_table",
        "list_metrics",
        "lookup_metric",
        "lookup_domain",
        "lookup_relationships",
        "trace_metric_impacts",
        "inspect_query",
        "run_query",
    }
    assert names == expected


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
    assert "Remaining:" in text


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
    assert "Remaining:" in text


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
    marker = "\nRemaining: "
    assert marker in text
    payload = json.loads(text.split(marker, 1)[1])
    assert "elapsed_seconds" in payload


@pytest.mark.asyncio
async def test_run_query_execute_exception_includes_remaining_budget(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    """Adapter execute exceptions should surface BLOCKED with Remaining: suffix."""
    from unittest.mock import patch

    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "run_query")

    # SQL that passes Layer 1 + EXPLAIN but raises at execute().
    with patch.object(
        adapter, "execute", side_effect=RuntimeError("simulated engine failure")
    ):
        result = await tool.callable(
            {"sql": "SELECT id FROM analytics.orders WHERE tenant_id = 'acme'"}
        )
    text = result["content"][0]["text"]
    assert "BLOCKED" in text
    assert "execution failed" in text.lower()
    assert "Remaining:" in text


@pytest.mark.asyncio
async def test_run_query_surfaces_log_messages() -> None:
    """enforcement=log rule should populate a LOG preamble in run_query output,
    mirroring the log_messages field inspect_query exposes."""
    from agentic_data_contracts.core.schema import (
        AllowedTable,
        DataContractSchema,
        Enforcement,
        QueryCheck,
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
                        name="tenant_filter_log",
                        description="Log when tenant_id filter is missing",
                        enforcement=Enforcement.LOG,
                        query_check=QueryCheck(required_filter="tenant_id"),
                    ),
                ],
            ),
        )
    )

    db = DuckDBAdapter(":memory:")
    db.connection.execute(
        """
        CREATE SCHEMA IF NOT EXISTS analytics;
        CREATE TABLE analytics.orders (id INTEGER);
        INSERT INTO analytics.orders VALUES (1);
        """
    )

    tools = create_tools(dc, adapter=db)
    tool = next(t for t in tools if t.name == "run_query")
    # No tenant_id filter — log rule should fire but not block execution.
    result = await tool.callable({"sql": "SELECT id FROM analytics.orders"})
    text = result["content"][0]["text"]

    # Query executed (rows returned) AND log preamble present.
    assert "LOG:" in text
    assert "tenant_id" in text
    # Payload JSON still present after the preamble.
    _, _, json_body = text.partition("\n\n")
    # Response may have both WARNINGS and LOG preambles; find the JSON tail.
    json_start = text.find("{")
    assert json_start != -1
    payload = json.loads(text[json_start:])
    assert payload["row_count"] == 1


@pytest.mark.asyncio
async def test_run_query_principal_denied_never_hits_database(
    fixtures_dir: Path,
) -> None:
    """A principal-denied query must not reach the database.

    Uses a spy that counts execute() calls on top of DuckDBAdapter; asserts
    that a blocked query leaves the count at zero.
    """
    from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
    from agentic_data_contracts.core.contract import DataContract
    from agentic_data_contracts.tools.factory import create_tools

    class SpyAdapter(DuckDBAdapter):
        def __init__(self, path: str) -> None:
            super().__init__(path)
            self.execute_calls: int = 0

        def execute(self, sql: str):  # type: ignore[override]
            self.execute_calls += 1
            return super().execute(sql)

    contract = DataContract.from_yaml(fixtures_dir / "principals_contract.yml")
    db = SpyAdapter(":memory:")
    db.connection.execute(
        "CREATE SCHEMA hr; "
        "CREATE TABLE hr.salaries (id INTEGER, salary DECIMAL(10,2)); "
        "INSERT INTO hr.salaries VALUES (1, 100000.00);"
    )

    tools = create_tools(contract, adapter=db, caller_principal="bob@co.com")
    run_query = next(t for t in tools if t.name == "run_query").callable

    response = await run_query({"sql": "SELECT salary FROM hr.salaries"})
    text = response["content"][0]["text"]

    assert "BLOCKED" in text
    assert "caller: 'bob@co.com'" in text
    assert db.execute_calls == 0, (
        f"Expected 0 execute() calls for a principal-denied query, "
        f"got {db.execute_calls}"
    )
