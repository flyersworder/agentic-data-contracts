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
from agentic_data_contracts.core.session import ContractSession
from agentic_data_contracts.semantic.yaml_source import YamlSource
from agentic_data_contracts.tools.factory import create_tools
from agentic_data_contracts.tools.sdk import (
    _wrap_with_session_check,
    create_sdk_mcp_server,
)


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


# ─── Session-limit alignment with LangChain adapter (v0.20.0) ─────────────────


@pytest.fixture
def contract_with_limits(fixtures_dir: Path) -> DataContract:
    """Real fixture contract with rules + max_retries=3 + tenant_id requirement —
    same pattern as tests/test_tools/test_langchain.py."""
    return DataContract.from_yaml(fixtures_dir / "valid_contract.yml")


@pytest.fixture
def semantic(fixtures_dir: Path) -> YamlSource:
    """Override valid_contract.yml's dbt-manifest reference with the
    in-tree YAML source."""
    return YamlSource(fixtures_dir / "semantic_source.yml")


@pytest.mark.asyncio
async def test_wrap_with_session_check_blocks_on_limit_exceeded(
    contract_with_limits: DataContract,
) -> None:
    """When the session has already exceeded its retry budget, the wrapped
    callable returns the canonical BLOCKED envelope without invoking
    the inner function. The envelope includes a ``Remaining:`` suffix so
    the agent can see remaining budget — matching ``run_query``'s own
    blocked-path format at ``factory.py:634-636``."""
    session = ContractSession(contract_with_limits)
    for _ in range(4):  # exceed max_retries=3
        session.record_retry()

    inner_called = False

    async def inner(args: dict) -> dict:
        nonlocal inner_called
        inner_called = True
        return {"content": [{"type": "text", "text": "should not run"}]}

    wrapped = _wrap_with_session_check(inner, session)
    result = await wrapped({"any": "args"})

    assert inner_called is False
    text = result["content"][0]["text"]
    assert text.startswith("BLOCKED — Session limit exceeded")
    # Agent must see remaining budget, same as run_query's self-emitted block.
    assert "Remaining:" in text
    assert "retries_remaining" in text or "elapsed_seconds" in text


@pytest.mark.asyncio
async def test_wrap_with_session_check_passes_through_when_within_limits(
    contract_with_limits: DataContract,
) -> None:
    """When limits are not exceeded, the wrapped callable delegates to
    the inner function and returns its result unchanged."""
    session = ContractSession(contract_with_limits)

    async def inner(args: dict) -> dict:
        return {"content": [{"type": "text", "text": "hello"}]}

    wrapped = _wrap_with_session_check(inner, session)
    result = await wrapped({})
    assert result["content"][0]["text"] == "hello"


@pytest.mark.asyncio
async def test_wrap_with_session_check_does_not_validate_sql(
    contract_with_limits: DataContract,
) -> None:
    """The session-check wrapper must NOT short-circuit on SQL validation —
    that would block inspect_query's reporting purpose. Only session-limit
    exhaustion triggers the BLOCKED envelope."""
    session = ContractSession(contract_with_limits)

    async def inner(args: dict) -> dict:
        # Simulate inspect_query's behavior: returns JSON describing
        # violations, never emits "BLOCKED —".
        return {
            "content": [
                {"type": "text", "text": '{"valid": false, "violations": ["x"]}'}
            ]
        }

    wrapped = _wrap_with_session_check(inner, session)
    result = await wrapped({"sql": "DELETE FROM analytics.orders"})
    text = result["content"][0]["text"]
    assert "BLOCKED" not in text
    assert "violations" in text


def test_create_sdk_mcp_server_accepts_apply_middleware_true(
    contract_with_limits: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    """v0.20.0: apply_middleware defaults to True. The new kwarg must be
    accepted and the server must build successfully."""
    server = create_sdk_mcp_server(
        contract_with_limits,
        adapter=adapter,
        semantic_source=semantic,
        apply_middleware=True,
    )
    assert server is not None


def test_create_sdk_mcp_server_accepts_apply_middleware_false(
    contract_with_limits: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    """Escape hatch — preserves pre-v0.20.0 behavior (no auto-wrapping)."""
    server = create_sdk_mcp_server(
        contract_with_limits,
        adapter=adapter,
        semantic_source=semantic,
        apply_middleware=False,
    )
    assert server is not None


def test_create_sdk_mcp_server_default_is_apply_middleware_true(
    contract_with_limits: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    """The v0.20.0 default is auto-apply, mirroring create_langchain_tools."""
    # Smoke check — server constructs without any apply_middleware= override.
    server = create_sdk_mcp_server(
        contract_with_limits, adapter=adapter, semantic_source=semantic
    )
    assert server is not None
