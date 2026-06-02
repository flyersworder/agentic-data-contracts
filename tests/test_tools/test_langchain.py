"""Tests for LangChain / deepagents BaseTool adapter."""

import threading
from pathlib import Path

import pytest

# Skip the entire module if the optional extra isn't installed —
# matches the "extra is optional" backward-compat contract.
pytest.importorskip("langchain_core")
pytest.importorskip("langchain")

from langchain_core.tools import BaseTool, ToolException  # noqa: E402

from agentic_data_contracts.adapters.duckdb import DuckDBAdapter  # noqa: E402
from agentic_data_contracts.core.contract import DataContract  # noqa: E402
from agentic_data_contracts.core.schema import (  # noqa: E402
    AllowedTable,
    DataContractSchema,
    SemanticConfig,
)
from agentic_data_contracts.core.session import ContractSession  # noqa: E402
from agentic_data_contracts.semantic.yaml_source import YamlSource  # noqa: E402
from agentic_data_contracts.tools.factory import create_tools  # noqa: E402
from agentic_data_contracts.tools.langchain import (  # noqa: E402
    ContractMiddleware,
    _unwrap_mcp_text,
    create_langchain_tools,
)

# ─── fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture
def contract(fixtures_dir: Path) -> DataContract:
    """Real fixture contract with rules + max_retries=3 + tenant_id requirement."""
    return DataContract.from_yaml(fixtures_dir / "valid_contract.yml")


@pytest.fixture
def contract_no_source() -> DataContract:
    """Minimal contract used by basic shape tests — avoids semantic source loading."""
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
def semantic(fixtures_dir: Path) -> YamlSource:
    """Override valid_contract.yml's dbt-manifest reference with the
    in-tree YAML source — same pattern as tests/test_tools/test_inspect_query.py."""
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


# ─── _unwrap_mcp_text helper ──────────────────────────────────────────────────


def test_unwrap_mcp_text_extracts_first_text_block() -> None:
    assert _unwrap_mcp_text({"content": [{"type": "text", "text": "hi"}]}) == "hi"


def test_unwrap_mcp_text_handles_empty_content() -> None:
    assert _unwrap_mcp_text({"content": []}) == ""


def test_unwrap_mcp_text_handles_missing_content_key() -> None:
    assert _unwrap_mcp_text({}) == ""


def test_unwrap_mcp_text_skips_non_text_blocks() -> None:
    env = {
        "content": [
            {"type": "image", "data": "..."},
            {"type": "text", "text": "ok"},
        ]
    }
    assert _unwrap_mcp_text(env) == "ok"


# ─── create_langchain_tools — shape ───────────────────────────────────────────


def test_create_langchain_tools_returns_nine_basetools(
    contract_no_source: DataContract, adapter: DuckDBAdapter
) -> None:
    tools = create_langchain_tools(contract_no_source, adapter=adapter)
    assert len(tools) == 9
    assert all(isinstance(t, BaseTool) for t in tools)


def test_create_langchain_tools_preserves_names(
    contract_no_source: DataContract, adapter: DuckDBAdapter
) -> None:
    tools = create_langchain_tools(contract_no_source, adapter=adapter)
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
    assert {t.name for t in tools} == expected


def test_create_langchain_tools_accepts_prebuilt_tooldefs(
    contract_no_source: DataContract, adapter: DuckDBAdapter
) -> None:
    tooldefs = create_tools(contract_no_source, adapter=adapter)
    lc_tools = create_langchain_tools(contract_no_source, tools=tooldefs)
    assert len(lc_tools) == 9


def test_run_query_args_schema_exposes_sql_property(
    contract_no_source: DataContract, adapter: DuckDBAdapter
) -> None:
    """Our JSON Schema dict must reach the agent verbatim — no Pydantic synth."""
    tools = create_langchain_tools(contract_no_source, adapter=adapter)
    run_query = next(t for t in tools if t.name == "run_query")
    schema = run_query.args_schema
    # langchain-core may store as dict or as a synthesized Pydantic model.
    assert schema is not None
    if isinstance(schema, dict):
        props = schema["properties"]
    else:
        props = schema.model_json_schema()["properties"]
    assert "sql" in props


# ─── enforcement: run_query gated SQL → ToolException ─────────────────────────


@pytest.mark.asyncio
async def test_run_query_blocked_sql_raises_tool_exception(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_langchain_tools(contract, adapter=adapter, semantic_source=semantic)
    run_query = next(t for t in tools if t.name == "run_query")
    with pytest.raises(ToolException) as exc:
        await run_query.ainvoke({"sql": "DELETE FROM analytics.orders"})
    assert "BLOCKED" in str(exc.value)


@pytest.mark.asyncio
async def test_run_query_allowed_sql_returns_content_and_artifact(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_langchain_tools(contract, adapter=adapter, semantic_source=semantic)
    run_query = next(t for t in tools if t.name == "run_query")
    # Invoking via ToolCall (vs raw kwargs) makes langchain wrap the
    # ``(content, artifact)`` tuple as a ToolMessage with both fields.
    result = await run_query.ainvoke(
        {
            "name": "run_query",
            "args": {
                "sql": (
                    "SELECT id, amount FROM analytics.orders WHERE tenant_id = 'acme'"
                ),
            },
            "id": "tc-allow",
            "type": "tool_call",
        }
    )
    assert "100" in result.content
    assert "BLOCKED" not in result.content
    assert isinstance(result.artifact, dict)
    assert "content" in result.artifact  # original MCP envelope preserved


# ─── enforcement: inspect_query reports violations as data, never blocks ──────


@pytest.mark.asyncio
async def test_inspect_query_returns_violations_as_json_does_not_raise(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    """inspect_query is meant to *report* violations as a structured JSON
    payload. The adapter must NOT auto-block it the way contract_middleware
    would; otherwise the agent loses its dry-run inspection capability."""
    tools = create_langchain_tools(contract, adapter=adapter, semantic_source=semantic)
    inspect = next(t for t in tools if t.name == "inspect_query")
    result = await inspect.ainvoke(
        {
            "name": "inspect_query",
            "args": {"sql": "DELETE FROM analytics.orders"},
            "id": "tc-inspect",
            "type": "tool_call",
        }
    )
    assert "violations" in result.content
    assert "BLOCKED" not in result.content


# ─── enforcement: non-SQL tools succeed when args are valid ───────────────────


@pytest.mark.asyncio
async def test_describe_table_allowed_does_not_raise(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_langchain_tools(contract, adapter=adapter, semantic_source=semantic)
    describe = next(t for t in tools if t.name == "describe_table")
    result = await describe.ainvoke(
        {
            "name": "describe_table",
            "args": {"schema": "analytics", "table": "orders"},
            "id": "tc-describe",
            "type": "tool_call",
        }
    )
    assert "BLOCKED" not in result.content


# ─── enforcement: session limits → ToolException across any tool ──────────────


@pytest.mark.asyncio
async def test_session_limit_exceeded_raises_tool_exception(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    """Even non-SQL tools must surface session-limit exhaustion. Fixture
    sets max_retries=3 (tests/fixtures/valid_contract.yml:45). The
    raised ToolException must include the ``Remaining:`` budget summary
    so the agent sees the same diagnostic info ``run_query`` would have
    emitted directly."""
    session = ContractSession(contract)
    for _ in range(4):  # exceed max_retries=3
        session.record_retry()
    tools = create_langchain_tools(
        contract, adapter=adapter, semantic_source=semantic, session=session
    )
    describe = next(t for t in tools if t.name == "describe_table")
    with pytest.raises(ToolException) as exc:
        await describe.ainvoke({"schema": "analytics", "table": "orders"})
    msg = str(exc.value).lower()
    assert "limit" in msg or "exceeded" in msg
    assert "remaining:" in msg


# ─── apply_middleware=False escape hatch ──────────────────────────────────────


@pytest.mark.asyncio
async def test_apply_middleware_false_skips_session_check(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    """With apply_middleware=False, the adapter does NOT pre-check session
    limits. Users are expected to install ContractMiddleware at the graph
    level instead. Verifies the escape hatch works for non-SQL tools."""
    session = ContractSession(contract)
    for _ in range(4):  # exhaust retries
        session.record_retry()
    tools = create_langchain_tools(
        contract,
        adapter=adapter,
        semantic_source=semantic,
        session=session,
        apply_middleware=False,
    )
    describe = next(t for t in tools if t.name == "describe_table")
    # Should NOT raise — describe_table doesn't self-check session limits.
    result = await describe.ainvoke(
        {
            "name": "describe_table",
            "args": {"schema": "analytics", "table": "orders"},
            "id": "tc-skip",
            "type": "tool_call",
        }
    )
    assert "BLOCKED" not in result.content


# ─── ContractMiddleware (graph-level integration) ─────────────────────────────


@pytest.mark.asyncio
async def test_contract_middleware_blocks_disallowed_sql_via_awrap_tool_call(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    """ContractMiddleware.awrap_tool_call must short-circuit a disallowed
    SQL with a ToolMessage(status='error') instead of letting the handler
    run."""
    from langchain.agents.middleware.types import ToolCallRequest
    from langchain_core.messages import ToolMessage

    mw = ContractMiddleware(contract, adapter=adapter)
    request = ToolCallRequest(
        tool_call={
            "name": "run_query",
            "args": {"sql": "DELETE FROM analytics.orders"},
            "id": "tc-1",
            "type": "tool_call",
        },
        tool=None,
        state={},
        runtime=None,  # ty: ignore[invalid-argument-type]
    )

    async def _handler(_req: ToolCallRequest) -> ToolMessage:  # pragma: no cover
        raise AssertionError("handler must not run when middleware blocks")

    result = await mw.awrap_tool_call(request, _handler)
    assert isinstance(result, ToolMessage)
    assert result.status == "error"
    assert "BLOCKED" in str(result.content)
    assert "Remaining:" in str(result.content)  # agent must see budget
    assert result.tool_call_id == "tc-1"


@pytest.mark.asyncio
async def test_contract_middleware_lets_allowed_sql_through(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    """When SQL passes validation, the middleware must delegate to the
    handler unchanged."""
    from langchain.agents.middleware.types import ToolCallRequest
    from langchain_core.messages import ToolMessage

    mw = ContractMiddleware(contract, adapter=adapter)
    request = ToolCallRequest(
        tool_call={
            "name": "run_query",
            "args": {
                "sql": ("SELECT id FROM analytics.orders WHERE tenant_id = 'acme'"),
            },
            "id": "tc-2",
            "type": "tool_call",
        },
        tool=None,
        state={},
        runtime=None,  # ty: ignore[invalid-argument-type]
    )

    expected = ToolMessage(content="ok", tool_call_id="tc-2")

    async def _handler(_req: ToolCallRequest) -> ToolMessage:
        return expected

    result = await mw.awrap_tool_call(request, _handler)
    assert result is expected


def test_contract_middleware_blocks_disallowed_sql_via_wrap_tool_call_sync(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    """Synchronous path coverage. ``deepagents`` runs an async loop, but
    ``wrap_tool_call`` is part of the public ``AgentMiddleware`` surface
    and must short-circuit equivalently. Mirrors the async test."""
    from langchain.agents.middleware.types import ToolCallRequest
    from langchain_core.messages import ToolMessage

    mw = ContractMiddleware(contract, adapter=adapter)
    request = ToolCallRequest(
        tool_call={
            "name": "run_query",
            "args": {"sql": "DELETE FROM analytics.orders"},
            "id": "tc-sync",
            "type": "tool_call",
        },
        tool=None,
        state={},
        runtime=None,  # ty: ignore[invalid-argument-type]
    )

    def _handler(_req: ToolCallRequest) -> ToolMessage:  # pragma: no cover
        raise AssertionError("handler must not run when middleware blocks")

    result = mw.wrap_tool_call(request, _handler)
    assert isinstance(result, ToolMessage)
    assert result.status == "error"
    assert "BLOCKED" in str(result.content)
    assert result.tool_call_id == "tc-sync"


@pytest.mark.asyncio
async def test_contract_middleware_offloads_validate_off_event_loop(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    """awrap_tool_call must run the blocking EXPLAIN dry-run (inside
    Validator.validate, via _check) on a worker thread, not the event-loop
    thread."""
    from langchain.agents.middleware.types import ToolCallRequest
    from langchain_core.messages import ToolMessage

    seen: dict[str, int] = {}
    original_explain = adapter.explain

    def tracking_explain(sql: str):  # type: ignore[no-untyped-def]
        seen["explain"] = threading.get_ident()
        return original_explain(sql)

    setattr(adapter, "explain", tracking_explain)

    mw = ContractMiddleware(contract, adapter=adapter)
    request = ToolCallRequest(
        tool_call={
            "name": "run_query",
            "args": {
                "sql": "SELECT id FROM analytics.orders WHERE tenant_id = 'acme'",
            },
            "id": "tc-thread",
            "type": "tool_call",
        },
        tool=None,
        state={},
        runtime=None,  # ty: ignore[invalid-argument-type]
    )

    async def _handler(_req: ToolCallRequest) -> ToolMessage:
        return ToolMessage(content="ok", tool_call_id="tc-thread")

    await mw.awrap_tool_call(request, _handler)
    assert seen["explain"] != threading.get_ident(), (
        "EXPLAIN ran on the event-loop thread"
    )


# ─── top-level lazy re-export ─────────────────────────────────────────────────


def test_top_level_imports_resolve_when_extra_installed() -> None:
    from agentic_data_contracts import (
        ContractMiddleware as _CM,
    )
    from agentic_data_contracts import (
        create_langchain_tools as _ct,
    )

    assert _CM is not None
    assert _ct is not None
