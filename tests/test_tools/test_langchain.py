"""Tests for LangChain / deepagents BaseTool adapter."""

import logging
import threading
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

# Skip the entire module if the optional extra isn't installed —
# matches the "extra is optional" backward-compat contract.
pytest.importorskip("langchain_core")
pytest.importorskip("langchain")

from langchain.agents.middleware.types import ToolCallRequest  # noqa: E402
from langchain.tools import ToolRuntime  # noqa: E402
from langchain_core.tools import BaseTool, ToolException  # noqa: E402

from agentic_data_contracts.adapters.duckdb import DuckDBAdapter  # noqa: E402
from agentic_data_contracts.core.contract import DataContract  # noqa: E402
from agentic_data_contracts.core.schema import (  # noqa: E402
    AllowedTable,
    DataContractSchema,
    ResourceConfig,
    SemanticConfig,
)
from agentic_data_contracts.core.session import ContractSession  # noqa: E402
from agentic_data_contracts.semantic.yaml_source import YamlSource  # noqa: E402
from agentic_data_contracts.tools.factory import create_tools  # noqa: E402
from agentic_data_contracts.tools.langchain import (  # noqa: E402
    ContractMiddleware,
    _observe_usage,
    _unwrap_mcp_text,
    _usage_scope_for,
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
        # langchain-core synthesizes a Pydantic v2 model here; ty widens
        # args_schema to a v1|v2 union, so guard the v2-only method.
        props = schema.model_json_schema()["properties"]  # ty: ignore[unresolved-attribute]
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


# ─── token budget (v0.34.0) ───────────────────────────────────────────────────


def _budgeted_contract() -> DataContract:
    return DataContract(
        DataContractSchema(
            name="budgeted",
            semantic=SemanticConfig(
                allowed_tables=[
                    AllowedTable.model_validate(
                        {"schema": "analytics", "tables": ["orders"]}
                    ),
                ],
            ),
            resources=ResourceConfig(token_budget=50_000),
        )
    )


@pytest.mark.parametrize("apply_middleware", [True, False])
def test_no_wiring_time_warning_now_that_this_path_observes_usage(
    caplog: pytest.LogCaptureFixture, adapter: DuckDBAdapter, apply_middleware: bool
) -> None:
    """v0.35.0 removed the warning by removing what it warned about.

    Both flag values are checked because the warning used to be gated on this
    flag -- `apply_middleware=False` meant "delegating to ContractMiddleware,
    do not cry wolf". With the tools themselves observing usage, neither value
    leaves a budget inert, so the gate went with the warning.
    """
    with caplog.at_level(logging.WARNING):
        create_langchain_tools(
            _budgeted_contract(), adapter=adapter, apply_middleware=apply_middleware
        )
    assert "token_budget" not in caplog.text


def _usage_request(
    total: int, thread_id: str | None = None, message_id: str | None = None
) -> ToolCallRequest:
    """A real ToolCallRequest carrying one usage-bearing AIMessage.

    ``thread_id`` mimics a configured checkpointer; ``message_id`` mimics the
    UUID LangGraph's reducer stamps on messages entering state, which is what
    separates conversations when no checkpointer is present.
    """
    from langchain_core.messages import AIMessage

    # `runtime` is typed non-optional on ToolCallRequest, but the middleware
    # reads it defensively: `None` stands for "outside a graph", the shape
    # where neither a thread id nor a runtime config is available.
    runtime = cast(
        Any,
        SimpleNamespace(config={"configurable": {"thread_id": thread_id}})
        if thread_id is not None
        else None,
    )
    return ToolCallRequest(
        tool_call={"name": "describe_table", "args": {}, "id": "1"},
        tool=cast(Any, None),
        state=cast(
            Any,
            {
                "messages": [
                    AIMessage(
                        id=message_id,
                        content="x",
                        usage_metadata={
                            "input_tokens": total - 300,
                            "output_tokens": 300,
                            "total_tokens": total,
                        },
                    )
                ]
            },
        ),
        runtime=runtime,
    )


def test_middleware_feeds_the_budget_from_run_state() -> None:
    # Reading run state is what makes a declared token_budget real. As of
    # v0.35.0 the StructuredTool path does it too, from the same helpers --
    # see the tool-path section below.
    session = ContractSession(_budgeted_contract())
    middleware = ContractMiddleware(_budgeted_contract(), session=session)

    middleware._observe_token_usage(_usage_request(1200, thread_id="T1"))
    assert session.tokens_used == 1200
    # Cumulative, not additive: observing the same history again must not
    # double-count it.
    middleware._observe_token_usage(_usage_request(1200, thread_id="T1"))
    assert session.tokens_used == 1200


def test_two_conversations_do_not_erase_each_other() -> None:
    """One middleware serves every thread, so the scope key must include it.

    With a constant scope, thread B's 1000 tokens vanish -- 1000 is not greater
    than A's 1000, so nothing accrues -- and B is then told it has its full
    budget left. That is the exact false-number defect this feature removes,
    reintroduced one layer down.
    """
    session = ContractSession(_budgeted_contract())
    middleware = ContractMiddleware(_budgeted_contract(), session=session)

    middleware._observe_token_usage(_usage_request(1000, thread_id="A"))
    middleware._observe_token_usage(_usage_request(1000, thread_id="B"))
    assert session.tokens_used == 2000


def test_missing_thread_id_still_accrues() -> None:
    # Neither a thread id nor an identified message: the last-resort key. Must
    # still feed rather than silently no-op.
    session = ContractSession(_budgeted_contract())
    middleware = ContractMiddleware(_budgeted_contract(), session=session)
    middleware._observe_token_usage(_usage_request(800))
    assert session.tokens_used == 800


def test_unthreaded_conversations_separate_by_message_id() -> None:
    """No checkpointer is the README's own LangChain wiring.

    LangGraph stamps a UUID on the first message as it enters state, so
    conversations are distinguishable even without a thread id. Without this,
    a second conversation whose running sum sits below the first's peak accrues
    nothing and is told it has its full budget — the same defect thread scoping
    fixed, one configuration over.
    """
    session = ContractSession(_budgeted_contract())
    middleware = ContractMiddleware(_budgeted_contract(), session=session)

    middleware._observe_token_usage(_usage_request(1000, message_id="conv-a"))
    middleware._observe_token_usage(_usage_request(1000, message_id="conv-b"))
    assert session.tokens_used == 2000


def test_thread_id_wins_over_message_id() -> None:
    # A configured checkpointer is the stronger signal: it survives history
    # trimming that could drop the first message entirely.
    request = _usage_request(10, thread_id="T", message_id="M")
    scope = _usage_scope_for(*ContractMiddleware._state_and_config(request))
    assert scope == "langchain:thread:T"


def test_nested_run_sharing_a_thread_id_under_counts() -> None:
    """A documented limitation, pinned so it cannot change unnoticed.

    A sub-agent or subgraph inherits its parent's ``thread_id`` but carries a
    shorter history of its own, so it collides on the scope key and the
    monotone guard drops its usage whole rather than mixing it: 5000 + 800
    accrues 5000. The obvious key extension does not work -- the config's
    ``checkpoint_ns`` is ``tools:<task-id>`` and changes on *every* tool call,
    so keying on it would re-accrue the full total each time, multiplying
    instead of separating.

    Asserted rather than merely commented because the failure is silent and in
    the safe direction, which is exactly the kind that survives for releases.
    """
    session = ContractSession(_budgeted_contract())
    state, config = ContractMiddleware._state_and_config(
        _usage_request(5000, thread_id="T1")
    )
    _observe_usage(session, state, config)

    nested_state, nested_config = ContractMiddleware._state_and_config(
        _usage_request(800, thread_id="T1")
    )
    _observe_usage(session, nested_state, nested_config)

    assert session.tokens_used == 5000  # true spend is 5800


def test_middleware_blocks_once_the_budget_is_spent() -> None:
    # The point of the whole feature: a breach must actually stop the next
    # tool call, not merely be counted.
    session = ContractSession(_budgeted_contract())
    middleware = ContractMiddleware(_budgeted_contract(), session=session)
    blocked = middleware._check(_usage_request(50_001, thread_id="A"))
    assert blocked is not None
    assert "token budget exceeded" in str(blocked.content)


# ─── token budget on the StructuredTool path (v0.35.0) ────────────────────────


def _tool_runtime(
    total: int, thread_id: str | None = None, message_id: str | None = None
) -> Any:
    """A real ToolRuntime carrying one usage-bearing AIMessage.

    The same run, described from the tool's side rather than the middleware's:
    ``ToolNode`` builds one of these per tool call and injects it into any
    parameter named ``runtime``.
    """
    from langchain_core.messages import AIMessage

    return ToolRuntime(
        state=cast(
            Any,
            {
                "messages": [
                    AIMessage(
                        id=message_id,
                        content="x",
                        usage_metadata={
                            "input_tokens": total - 300,
                            "output_tokens": 300,
                            "total_tokens": total,
                        },
                    )
                ]
            },
        ),
        context=None,
        config=cast(
            Any, {"configurable": {"thread_id": thread_id}} if thread_id else {}
        ),
        stream_writer=cast(Any, lambda _: None),
        tool_call_id="call-1",
        store=None,
    )


def _budgeted_query_tool(
    session: ContractSession, adapter: DuckDBAdapter, **kwargs: Any
) -> BaseTool:
    tools = create_langchain_tools(
        _budgeted_contract(), adapter=adapter, session=session, **kwargs
    )
    return next(t for t in tools if t.name == "run_query")


_QUERY = {"sql": "SELECT * FROM analytics.orders"}


async def test_structured_tool_feeds_the_budget_from_run_state(
    adapter: DuckDBAdapter,
) -> None:
    """The gap this release closes: no middleware, budget still enforced."""
    session = ContractSession(_budgeted_contract())
    tool = _budgeted_query_tool(session, adapter)

    await tool.ainvoke({**_QUERY, "runtime": _tool_runtime(1200, thread_id="T1")})
    assert session.tokens_used == 1200
    # Cumulative, not additive: re-observing the same history must not
    # double-count it.
    await tool.ainvoke({**_QUERY, "runtime": _tool_runtime(1200, thread_id="T1")})
    assert session.tokens_used == 1200


async def test_structured_tool_keeps_two_conversations_apart(
    adapter: DuckDBAdapter,
) -> None:
    """One tool list serves every conversation, exactly as one middleware does.

    A constant scope key drops thread B entirely -- 1000 is not greater than
    A's 1000 -- and then reports B its full budget. That defect was caught on
    the middleware during the v0.34.0 review; sharing the scope derivation is
    what stops it being reintroduced here.
    """
    session = ContractSession(_budgeted_contract())
    tool = _budgeted_query_tool(session, adapter)

    await tool.ainvoke({**_QUERY, "runtime": _tool_runtime(1000, thread_id="A")})
    await tool.ainvoke({**_QUERY, "runtime": _tool_runtime(1000, thread_id="B")})
    assert session.tokens_used == 2000


async def test_structured_tool_separates_unthreaded_conversations(
    adapter: DuckDBAdapter,
) -> None:
    # No checkpointer is the README's own LangChain wiring; the first message's
    # reducer-assigned UUID is what distinguishes conversations there.
    session = ContractSession(_budgeted_contract())
    tool = _budgeted_query_tool(session, adapter)

    await tool.ainvoke({**_QUERY, "runtime": _tool_runtime(1000, message_id="conv-a")})
    await tool.ainvoke({**_QUERY, "runtime": _tool_runtime(1000, message_id="conv-b")})
    assert session.tokens_used == 2000


async def test_structured_tool_blocks_once_the_budget_is_spent(
    adapter: DuckDBAdapter,
) -> None:
    """Observation precedes the limit check, so the breaching run stops itself.

    Checking first would let the offending call through and block the *next*
    one -- and if the agent stops calling tools after it, never block at all.
    """
    session = ContractSession(_budgeted_contract())
    tool = _budgeted_query_tool(session, adapter)

    with pytest.raises(ToolException) as excinfo:
        await tool.ainvoke({**_QUERY, "runtime": _tool_runtime(50_001, thread_id="A")})
    assert "token budget exceeded" in str(excinfo.value)


async def test_direct_invocation_without_a_runtime_still_works(
    adapter: DuckDBAdapter,
) -> None:
    """``runtime`` is injected by ToolNode, not by ``ainvoke``.

    Calling a tool directly is a supported shape -- most of this file does it --
    so the parameter has to be optional. Nothing is observed, which is exactly
    the pre-v0.35.0 behaviour rather than a new failure.
    """
    session = ContractSession(_budgeted_contract())
    tool = _budgeted_query_tool(session, adapter)

    result = await tool.ainvoke(_QUERY)
    assert "acme" in str(result)
    assert session.tokens_used == 0


def _scripted_agent(tools: list[BaseTool], turn_tokens: int, **kwargs: Any) -> Any:
    """A real ``create_agent`` whose model is scripted to call ``run_query`` once.

    Real graph, real ``ToolNode``, real injection — only the model is faked,
    and only so the tool call and its ``usage_metadata`` are deterministic.
    ``bind_tools`` is overridden because the fake does not implement it;
    ``create_agent`` only needs the bound object back, and binding happens
    upstream of injection either way.
    """
    from langchain.agents import create_agent
    from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
    from langchain_core.messages import AIMessage

    class _ToolCallingModel(GenericFakeChatModel):
        def bind_tools(self, tools: Any, **kw: Any) -> Any:
            return self

    replies = iter(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "run_query",
                        "args": _QUERY,
                        "id": "c1",
                        "type": "tool_call",
                    }
                ],
                usage_metadata={
                    "input_tokens": turn_tokens - 300,
                    "output_tokens": 300,
                    "total_tokens": turn_tokens,
                },
            ),
            AIMessage(content="1 row."),
        ]
    )
    return create_agent(
        model=_ToolCallingModel(messages=replies), tools=tools, **kwargs
    )


async def test_budget_is_fed_through_a_real_agent(adapter: DuckDBAdapter) -> None:
    """The load-bearing test: does ToolNode actually inject the runtime?

    Every other test in this section hands ``runtime`` over itself, so all of
    them would still pass if injection never happened -- and injection is the
    entire feature. It hinges on something invisible at the call site:
    ``ToolNode`` runs ``get_type_hints()`` over the coroutine to decide whether
    to inject, and because ``tools/langchain.py`` uses ``from __future__ import
    annotations`` that resolution needs ``ToolRuntime`` in the module's globals.

    Verified by mutation, twice. Moving that import into function scope fails
    this test and no other (loudly -- ``NameError`` out of the hint
    evaluation). Dropping the parameter's annotation also fails this test and
    no other, and *that* one is silent. Renaming the parameter, contrary to
    what one might assume, does not break injection at all: ToolNode triggers
    on the name ``runtime`` **or** the ``ToolRuntime`` annotation, so the
    annotation is the half that carries the feature.
    """
    session = ContractSession(_budgeted_contract())
    agent = _scripted_agent(
        create_langchain_tools(_budgeted_contract(), adapter=adapter, session=session),
        turn_tokens=1000,
    )

    result = await agent.ainvoke(
        {"messages": [("user", "count the orders")]},
        config={"configurable": {"thread_id": "T1"}},
    )

    # The tool ran, and the assistant turn that requested it was billed to the
    # session -- which before this release stayed at zero forever.
    assert any(m.type == "tool" for m in result["messages"])
    assert session.tokens_used == 1000


async def test_tools_and_middleware_on_one_session_do_not_double_count(
    adapter: DuckDBAdapter,
) -> None:
    """Wiring both is legal, so it must not halve the effective budget.

    Driven through a real graph on purpose. Asserting this against two
    hand-built inputs would only prove ``observe_tokens`` is idempotent for an
    equal ``(scope, total)`` pair, which was never in doubt -- the claim worth
    guarding is that the two paths *derive* the same scope and the same total
    from one run, having reached it by different routes (``request.state`` and
    ``request.runtime.config`` versus ``ToolRuntime.state`` and
    ``.config``). That derivation agreeing is what makes the tool-path
    observation safe to leave unconditional instead of gating it on
    ``apply_middleware``, which governs enforcement rather than observation.
    """
    session = ContractSession(_budgeted_contract())
    agent = _scripted_agent(
        create_langchain_tools(
            _budgeted_contract(),
            adapter=adapter,
            session=session,
            apply_middleware=False,
        ),
        turn_tokens=1000,
        middleware=[ContractMiddleware(_budgeted_contract(), session=session)],
    )

    await agent.ainvoke(
        {"messages": [("user", "count the orders")]},
        config={"configurable": {"thread_id": "T1"}},
    )

    # Both observed; counted once. 2000 here would mean the scope keys or the
    # totals diverged between the two paths.
    assert session.tokens_used == 1000


async def test_both_paths_agree_without_a_checkpointer(adapter: DuckDBAdapter) -> None:
    """The same agreement, on the ``conv:`` branch of the scope key.

    Without a ``thread_id`` the key falls back to the id LangGraph stamps on
    the first message -- a different branch of ``_usage_scope_for``, and the
    README's own LangChain wiring. Both paths must land on the same id.
    """
    session = ContractSession(_budgeted_contract())
    agent = _scripted_agent(
        create_langchain_tools(
            _budgeted_contract(),
            adapter=adapter,
            session=session,
            apply_middleware=False,
        ),
        turn_tokens=1000,
        middleware=[ContractMiddleware(_budgeted_contract(), session=session)],
    )

    await agent.ainvoke({"messages": [("user", "count the orders")]})

    assert session.tokens_used == 1000


def test_runtime_is_never_advertised_to_the_model(adapter: DuckDBAdapter) -> None:
    """``infer_schema=False`` means the dict schema is the schema.

    If the coroutine signature ever started driving the advertised arguments,
    the model would see a ``runtime`` parameter it cannot fill and would try.
    """
    session = ContractSession(_budgeted_contract())
    tool = _budgeted_query_tool(session, adapter)
    assert "runtime" not in tool.args
    assert set(tool.args) == {"sql"}
