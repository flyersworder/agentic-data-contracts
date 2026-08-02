"""MCP `is_error` signalling on tool results.

``claude_agent_sdk`` reads ``is_error`` off the returned envelope and maps it to
MCP's ``isError``, which the spec designates as the channel for tool execution
errors the model can self-correct from. Without it every governance decision —
a forbidden operation, a missing tenant filter, a restricted table — reaches the
model as a *successful* tool result.

The rule: ``is_error`` means the tool did not perform the action it advertises.
A lookup that legitimately found nothing is a valid answer, not an error.
"""

from pathlib import Path
from typing import Any

import pytest

from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.semantic.yaml_source import YamlSource
from agentic_data_contracts.tools.factory import (
    _PROTOCOL_PRECEDENCE,
    ToolDef,
    create_tools,
)


@pytest.fixture
def contract(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml(fixtures_dir / "valid_contract.yml")


@pytest.fixture
def semantic(fixtures_dir: Path) -> YamlSource:
    return YamlSource(fixtures_dir / "semantic_source.yml")


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


def _tool(tools: list[ToolDef], name: str) -> ToolDef:
    return next(t for t in tools if t.name == name)


def _text(envelope: dict[str, Any]) -> str:
    return str(envelope["content"][0]["text"])


# ── Errors: the tool did not do what it advertises ──────────────────────────


@pytest.mark.asyncio
async def test_disallowed_table_is_an_error(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    for name in ("describe_table", "preview_table"):
        result = await _tool(tools, name).callable(
            {"schema": "forbidden", "table": "secrets"}
        )
        assert result.get("is_error") is True, name
        assert "not in the allowed tables list" in _text(result)


@pytest.mark.asyncio
async def test_blocked_query_is_an_error(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    # valid_contract blocks on a missing tenant_id filter.
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    result = await _tool(tools, "run_query").callable(
        {"sql": "SELECT id FROM analytics.orders"}
    )
    assert result.get("is_error") is True
    assert _text(result).startswith("BLOCKED —")


@pytest.mark.asyncio
async def test_forbidden_operation_is_an_error(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    result = await _tool(tools, "run_query").callable(
        {"sql": "DELETE FROM analytics.orders WHERE tenant_id = 'acme'"}
    )
    assert result.get("is_error") is True


@pytest.mark.asyncio
async def test_missing_adapter_is_an_error(
    contract: DataContract, semantic: YamlSource
) -> None:
    # Misconfiguration: the call cannot be performed at all.
    tools = create_tools(contract, semantic_source=semantic)
    result = await _tool(tools, "run_query").callable(
        {"sql": "SELECT id FROM analytics.orders WHERE tenant_id = 'acme'"}
    )
    assert result.get("is_error") is True
    assert "No database adapter configured" in _text(result)


@pytest.mark.asyncio
async def test_missing_semantic_source_is_an_error(
    fixtures_dir: Path, adapter: DuckDBAdapter
) -> None:
    contract = DataContract.from_yaml(fixtures_dir / "minimal_contract.yml")
    tools = create_tools(contract, adapter=adapter)
    result = await _tool(tools, "list_metrics").callable({})
    assert result.get("is_error") is True
    assert _text(result) == "No semantic source configured."


@pytest.mark.asyncio
async def test_invalid_argument_is_an_error(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    # MCP names input validation errors as tool execution errors — the model
    # can self-correct by passing a valid value.
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    result = await _tool(tools, "trace_metric_impacts").callable(
        {"metric_name": "total_revenue", "direction": "sideways"}
    )
    assert result.get("is_error") is True


# ── Not errors: a valid answer that happens to be negative ──────────────────


@pytest.mark.asyncio
async def test_metric_not_found_is_not_an_error(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    # A lookup that found nothing answered the question it was asked. Flagging
    # it would tell the model its call failed and distort fuzzy-search recovery.
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    result = await _tool(tools, "lookup_metric").callable(
        {"metric_name": "zzz_no_such_metric_zzz"}
    )
    assert result.get("is_error") is not True


@pytest.mark.asyncio
async def test_inspect_query_reporting_violations_is_not_an_error(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    # Reporting violations is precisely what inspect_query is for; the
    # inspection succeeded.
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    result = await _tool(tools, "inspect_query").callable(
        {"sql": "SELECT id FROM analytics.orders"}
    )
    assert result.get("is_error") is not True


@pytest.mark.asyncio
async def test_successful_query_is_not_an_error(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    result = await _tool(tools, "run_query").callable(
        {"sql": "SELECT id FROM analytics.orders WHERE tenant_id = 'acme'"}
    )
    assert result.get("is_error") is not True


# ── Invariant ────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_every_blocked_envelope_carries_is_error(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    # This is the guard that earns its keep: a future BLOCKED site added without
    # the flag is exactly how the gap arose in the first place. Any envelope
    # whose text announces a block must also say so structurally.
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    calls: list[tuple[str, dict[str, Any]]] = [
        ("run_query", {"sql": "SELECT id FROM analytics.orders"}),  # no tenant
        ("run_query", {"sql": "SELECT * FROM analytics.orders"}),  # SELECT *
        ("run_query", {"sql": "DROP TABLE analytics.orders"}),  # forbidden op
        ("run_query", {"sql": "!! not sql at all"}),  # unparseable
    ]
    seen_blocked = False
    for name, args in calls:
        result = await _tool(tools, name).callable(args)
        if _text(result).startswith("BLOCKED —"):
            seen_blocked = True
            assert result.get("is_error") is True, f"{name} {args}"
    assert seen_blocked, "no BLOCKED envelope produced — invariant untested"


@pytest.mark.asyncio
async def test_middleware_envelopes_carry_is_error(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    # contract_middleware is the BYO-tool path, exported from the package root.
    # Its envelopes are built by hand rather than through _error_response, so
    # they need their own coverage -- the previous version of this invariant
    # spanned only create_tools, which is exactly how its violations envelope
    # shipped without the flag.
    from agentic_data_contracts.tools.middleware import contract_middleware

    @contract_middleware(contract, adapter=adapter)
    async def my_query(args: dict[str, Any]) -> dict[str, Any]:  # pragma: no cover
        return {"content": [{"type": "text", "text": "ok"}]}

    blocked = await my_query({"sql": "SELECT id FROM analytics.orders"})
    assert _text(blocked).startswith("BLOCKED —")
    assert blocked.get("is_error") is True


@pytest.mark.asyncio
async def test_middleware_session_limit_carries_is_error(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    from agentic_data_contracts.core.session import ContractSession
    from agentic_data_contracts.tools.middleware import contract_middleware

    session = ContractSession(contract)
    session.record_retry()
    session.record_retry()
    session.record_retry()
    session.record_retry()

    @contract_middleware(contract, adapter=adapter, session=session)
    async def my_query(args: dict[str, Any]) -> dict[str, Any]:  # pragma: no cover
        return {"content": [{"type": "text", "text": "ok"}]}

    result = await my_query({"sql": "SELECT id FROM analytics.orders"})
    assert "Session limit exceeded" in _text(result)
    assert result.get("is_error") is True


@pytest.mark.asyncio
async def test_sdk_session_check_envelope_carries_is_error(
    contract: DataContract,
) -> None:
    from agentic_data_contracts.core.session import ContractSession
    from agentic_data_contracts.tools.sdk import _wrap_with_session_check

    session = ContractSession(contract)
    for _ in range(4):
        session.record_retry()

    async def inner(args: dict[str, Any]) -> dict[str, Any]:  # pragma: no cover
        return {"content": [{"type": "text", "text": "ok"}]}

    result = await _wrap_with_session_check(inner, session)({})
    assert "Session limit exceeded" in _text(result)
    assert result.get("is_error") is True


@pytest.mark.asyncio
async def test_run_query_session_limit_carries_is_error(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    from agentic_data_contracts.core.session import ContractSession

    session = ContractSession(contract)
    for _ in range(4):
        session.record_retry()
    tools = create_tools(
        contract, adapter=adapter, semantic_source=semantic, session=session
    )
    result = await _tool(tools, "run_query").callable(
        {"sql": "SELECT id FROM analytics.orders WHERE tenant_id = 'acme'"}
    )
    assert "Session limit exceeded" in _text(result)
    assert result.get("is_error") is True


# ── MCP tool annotations (SDK path only) ─────────────────────────────────────


def test_read_only_annotation_covers_every_non_executing_tool(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    # Driven from create_tools() output rather than a hardcoded list, so a
    # renamed or newly added tool fails here instead of silently losing its
    # annotation.
    pytest.importorskip("claude_agent_sdk")
    from agentic_data_contracts.tools.sdk import _annotations_for

    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    for tool in tools:
        annotations = _annotations_for(tool.name)
        if tool.name == "run_query":
            # Left unset: whether run_query can write depends on the contract,
            # and `CALL` / vendor DDL still parse as a bare Command, so claiming
            # read-only would overclaim. Absent means "unknown" in MCP.
            assert annotations is None
        else:
            assert annotations is not None, tool.name
            assert annotations.readOnlyHint is True, tool.name


def test_read_only_set_matches_the_shipped_tools(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    # Guards the other direction: a name in the set that no longer exists is
    # dead config that would quietly stop applying.
    pytest.importorskip("claude_agent_sdk")
    from agentic_data_contracts.tools.sdk import _READ_ONLY_TOOLS

    shipped = {
        t.name
        for t in create_tools(contract, adapter=adapter, semantic_source=semantic)
    }
    assert _READ_ONLY_TOOLS <= shipped
    assert shipped - _READ_ONLY_TOOLS == {"run_query"}


def test_sdk_forwards_description_and_annotations(
    contract: DataContract,
    adapter: DuckDBAdapter,
    semantic: YamlSource,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pin the SDK wiring without touching MCP internals.

    The registered MCP `Server` exposes no public read-back for a tool's
    description or annotations, so asserting through it would couple this suite
    to a third-party private API. But `sdk.py` imports `claude_agent_sdk.tool`
    *inside* the function, so spying on that public name captures exactly what
    was forwarded. The unit tests above cover `_annotations_for`'s lookup; this
    covers the call.
    """
    claude_agent_sdk = pytest.importorskip("claude_agent_sdk")
    from agentic_data_contracts.tools.sdk import create_sdk_mcp_server

    seen: dict[str, tuple[str, Any]] = {}
    real_tool = claude_agent_sdk.tool

    def spy(name: str, description: str, input_schema: Any, annotations: Any = None):
        seen[name] = (description, annotations)
        return real_tool(name, description, input_schema, annotations)

    monkeypatch.setattr(claude_agent_sdk, "tool", spy)
    create_sdk_mcp_server(contract, adapter=adapter, semantic_source=semantic)

    run_query_description, run_query_annotations = seen["run_query"]
    assert _PROTOCOL_PRECEDENCE in run_query_description
    assert run_query_annotations is None

    _, describe_annotations = seen["describe_table"]
    assert describe_annotations is not None
    assert describe_annotations.readOnlyHint is True
