"""``_kind`` on `_error_response` envelopes.

Classifies *why* a tool returned an error envelope, distinct from
``is_error`` (whether it should be one at all). ``kind="blocked"`` marks a
governance denial — the contract refused the action; the default
``kind="error"`` covers misconfiguration, invalid arguments, and execution
failures. A later task (the conformance recorder) reads ``_kind`` to
classify a tool call as blocked vs. error rather than treating every
non-raising return as a successful call.

The unit tests below pin ``_error_response``'s own contract with literal
strings; they cannot catch a call site drifting to the wrong ``kind`` (or
losing it entirely), since that requires invoking the actual tool callable
and reading ``_kind`` off its envelope. The call-site tests further down do
that, across both classes and multiple tools — a silent misclassification
there would not fail loudly, it would just make every downstream protocol
verdict wrong while still looking green.
"""

from pathlib import Path
from typing import Any

import pytest

from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.session import ContractSession
from agentic_data_contracts.semantic.yaml_source import YamlSource
from agentic_data_contracts.tools.factory import ToolDef, _error_response, create_tools

# ── `_error_response`'s own contract (literal strings) ───────────────────────


def test_defaults_to_error_kind() -> None:
    assert _error_response("boom")["_kind"] == "error"


def test_blocked_kind_is_carried() -> None:
    assert _error_response("BLOCKED — nope", kind="blocked")["_kind"] == "blocked"


def test_is_error_is_still_set_for_mcp() -> None:
    assert _error_response("boom")["is_error"] is True


# ── Call-site classifications, exercised through real tool calls ────────────


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
        """
    )
    return db


@pytest.fixture
def semantic(fixtures_dir: Path) -> YamlSource:
    return YamlSource(fixtures_dir / "semantic_source.yml")


def _tool(tools: list[ToolDef], name: str) -> ToolDef:
    return next(t for t in tools if t.name == name)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tool_name", "args"),
    [
        (
            "describe_table",
            {"schema": "forbidden", "table": "secrets"},
        ),
        (
            # Missing the required tenant_id filter — a block-enforcement
            # query_check in valid_contract.yml's tenant_isolation rule.
            "run_query",
            {"sql": "SELECT id FROM analytics.orders"},
        ),
    ],
)
async def test_governance_denials_are_blocked(
    contract: DataContract,
    adapter: DuckDBAdapter,
    semantic: YamlSource,
    tool_name: str,
    args: dict[str, Any],
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    result = await _tool(tools, tool_name).callable(args)
    assert result["_kind"] == "blocked"


@pytest.mark.asyncio
async def test_run_query_session_limit_exhausted_is_blocked(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    session = ContractSession(contract)
    for _ in range(4):  # valid_contract.yml sets max_retries: 3
        session.record_retry()
    tools = create_tools(
        contract, adapter=adapter, semantic_source=semantic, session=session
    )
    result = await _tool(tools, "run_query").callable(
        {"sql": "SELECT id FROM analytics.orders WHERE tenant_id = 'acme'"}
    )
    assert result["_kind"] == "blocked"


@pytest.mark.asyncio
async def test_run_query_no_adapter_is_error(
    contract: DataContract, semantic: YamlSource
) -> None:
    tools = create_tools(contract, semantic_source=semantic)
    result = await _tool(tools, "run_query").callable(
        {"sql": "SELECT id FROM analytics.orders WHERE tenant_id = 'acme'"}
    )
    assert result["_kind"] == "error"


@pytest.mark.asyncio
async def test_lookup_metric_no_semantic_source_is_error(
    fixtures_dir: Path, adapter: DuckDBAdapter
) -> None:
    # minimal_contract.yml, not the module's `contract` fixture: valid_contract.yml
    # declares a dbt semantic.source that create_tools would try (and fail) to
    # auto-load, rather than leaving semantic_source unset as this case needs.
    contract = DataContract.from_yaml(fixtures_dir / "minimal_contract.yml")
    tools = create_tools(contract, adapter=adapter)
    result = await _tool(tools, "lookup_metric").callable(
        {"metric_name": "total_revenue"}
    )
    assert result["_kind"] == "error"


@pytest.mark.asyncio
async def test_trace_metric_impacts_invalid_direction_is_error(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    result = await _tool(tools, "trace_metric_impacts").callable(
        {"metric_name": "total_revenue", "direction": "sideways"}
    )
    assert result["_kind"] == "error"
