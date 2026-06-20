"""Tests for the Pydantic AI Tool adapter."""

from pathlib import Path
from typing import Any, cast

import pytest

# Skip the entire module if the optional extra isn't installed —
# matches the "extra is optional" backward-compat contract.
pytest.importorskip("pydantic_ai")

from pydantic_ai import Agent, ModelRetry, RunContext, Tool  # noqa: E402
from pydantic_ai.models.test import TestModel  # noqa: E402
from pydantic_ai.usage import RunUsage  # noqa: E402

from agentic_data_contracts.adapters.duckdb import DuckDBAdapter  # noqa: E402
from agentic_data_contracts.core.contract import DataContract  # noqa: E402
from agentic_data_contracts.core.schema import (  # noqa: E402
    AllowedTable,
    DataContractSchema,
    SemanticConfig,
)
from agentic_data_contracts.core.session import (  # noqa: E402
    ContractSession,
    ContractSessionLimitError,
)
from agentic_data_contracts.semantic.yaml_source import YamlSource  # noqa: E402
from agentic_data_contracts.tools.factory import create_tools  # noqa: E402
from agentic_data_contracts.tools.pydantic_ai import (  # noqa: E402
    ContractDeps,
    _unwrap_mcp_text,
    create_pydantic_ai_tools,
    create_pydantic_ai_toolset,
)

# ─── helpers ──────────────────────────────────────────────────────────────────


async def _invoke(tool: Tool, **kwargs: Any) -> Any:
    """Call a wrapped tool's underlying function with keyword args.

    ``Tool.function``'s declared type carries pydantic_ai's positional
    ``RunContext`` signature, while ours is ``_fn(**kwargs)``. Going through
    an ``Any`` indirection lets tests invoke it directly without the type
    checker flagging the call shape.
    """
    fn = cast(Any, tool.function)
    return await fn(**kwargs)


def _run_ctx(deps: Any) -> RunContext[Any]:
    """Minimal RunContext for driving a deps-aware toolset factory directly."""
    return RunContext(deps=deps, model=TestModel(), usage=RunUsage())


def _toolset_tools(factory: Any, deps: Any) -> dict[str, Tool]:
    """Invoke the deps-aware factory with ``deps`` and return its {name: Tool}."""
    toolset = factory(_run_ctx(deps))
    return cast("dict[str, Tool]", toolset.tools)


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
    """Override valid_contract.yml's dbt-manifest reference with the in-tree
    YAML source — same pattern as the LangChain/SDK adapter tests."""
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


# ─── create_pydantic_ai_tools — shape ─────────────────────────────────────────


def test_create_pydantic_ai_tools_returns_nine_tools(
    contract_no_source: DataContract, adapter: DuckDBAdapter
) -> None:
    tools = create_pydantic_ai_tools(contract_no_source, adapter=adapter)
    assert len(tools) == 9
    assert all(isinstance(t, Tool) for t in tools)


def test_create_pydantic_ai_tools_preserves_names(
    contract_no_source: DataContract, adapter: DuckDBAdapter
) -> None:
    tools = create_pydantic_ai_tools(contract_no_source, adapter=adapter)
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


def test_create_pydantic_ai_tools_accepts_prebuilt_tooldefs(
    contract_no_source: DataContract, adapter: DuckDBAdapter
) -> None:
    tooldefs = create_tools(contract_no_source, adapter=adapter)
    pai_tools = create_pydantic_ai_tools(contract_no_source, tools=tooldefs)
    assert len(pai_tools) == 9


def test_run_query_schema_exposes_sql_property_verbatim(
    contract_no_source: DataContract, adapter: DuckDBAdapter
) -> None:
    """Our JSON Schema dict must reach the model verbatim — Tool.from_schema
    stores it on function_schema.json_schema without Pydantic synthesis."""
    tools = create_pydantic_ai_tools(contract_no_source, adapter=adapter)
    run_query = next(t for t in tools if t.name == "run_query")
    props = run_query.function_schema.json_schema["properties"]
    assert "sql" in props


# ─── enforcement: run_query gated SQL → ModelRetry (recoverable) ──────────────


@pytest.mark.asyncio
async def test_run_query_blocked_sql_raises_model_retry(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    """A blocked query is recoverable: the model should rewrite and retry,
    so the adapter raises ModelRetry (not the terminal error)."""
    tools = create_pydantic_ai_tools(
        contract, adapter=adapter, semantic_source=semantic
    )
    run_query = next(t for t in tools if t.name == "run_query")
    with pytest.raises(ModelRetry) as exc:
        await _invoke(run_query, sql="DELETE FROM analytics.orders")
    assert "BLOCKED" in str(exc.value)


@pytest.mark.asyncio
async def test_run_query_allowed_sql_returns_text(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_pydantic_ai_tools(
        contract, adapter=adapter, semantic_source=semantic
    )
    run_query = next(t for t in tools if t.name == "run_query")
    result = await _invoke(
        run_query,
        sql="SELECT id, amount FROM analytics.orders WHERE tenant_id = 'acme'",
    )
    assert isinstance(result, str)
    assert "100" in result
    assert "BLOCKED" not in result


# ─── enforcement: inspect_query reports violations as data, never blocks ──────


@pytest.mark.asyncio
async def test_inspect_query_reports_violations_as_json_does_not_raise(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    """inspect_query *reports* violations as JSON; the adapter must not
    convert that into a ModelRetry, or the agent loses dry-run inspection."""
    tools = create_pydantic_ai_tools(
        contract, adapter=adapter, semantic_source=semantic
    )
    inspect = next(t for t in tools if t.name == "inspect_query")
    result = await _invoke(inspect, sql="DELETE FROM analytics.orders")
    assert "violations" in result
    assert "BLOCKED" not in result


# ─── enforcement: non-SQL tools succeed when args are valid ───────────────────


@pytest.mark.asyncio
async def test_describe_table_allowed_does_not_raise(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_pydantic_ai_tools(
        contract, adapter=adapter, semantic_source=semantic
    )
    describe = next(t for t in tools if t.name == "describe_table")
    result = await _invoke(describe, schema="analytics", table="orders")
    assert "BLOCKED" not in result


# ─── enforcement: session limits → terminal error (NOT ModelRetry) ────────────


@pytest.mark.asyncio
async def test_session_limit_exceeded_raises_terminal_not_model_retry(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    """Session-limit exhaustion is terminal: retrying can't help, so the
    adapter raises ContractSessionLimitError (a RuntimeError), NOT ModelRetry.
    Fixture sets max_retries=3 (tests/fixtures/valid_contract.yml). The error
    must carry the ``Remaining:`` budget summary run_query would emit."""
    session = ContractSession(contract)
    for _ in range(4):  # exceed max_retries=3
        session.record_retry()
    tools = create_pydantic_ai_tools(
        contract, adapter=adapter, semantic_source=semantic, session=session
    )
    describe = next(t for t in tools if t.name == "describe_table")
    with pytest.raises(ContractSessionLimitError) as exc:
        await _invoke(describe, schema="analytics", table="orders")
    assert not isinstance(exc.value, ModelRetry)
    msg = str(exc.value).lower()
    assert "limit" in msg or "exceeded" in msg
    assert "remaining:" in msg


# ─── apply_middleware=False escape hatch ──────────────────────────────────────


@pytest.mark.asyncio
async def test_apply_middleware_false_skips_session_check(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    """With apply_middleware=False the adapter does not pre-check session
    limits, so a non-SQL tool runs even on an exhausted session."""
    session = ContractSession(contract)
    for _ in range(4):  # exhaust retries
        session.record_retry()
    tools = create_pydantic_ai_tools(
        contract,
        adapter=adapter,
        semantic_source=semantic,
        session=session,
        apply_middleware=False,
    )
    describe = next(t for t in tools if t.name == "describe_table")
    result = await _invoke(describe, schema="analytics", table="orders")
    assert "BLOCKED" not in result


@pytest.mark.asyncio
async def test_run_query_session_limit_terminal_even_without_middleware(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    """Even with apply_middleware=False, run_query self-checks limits and
    emits its own ``BLOCKED — Session limit exceeded`` envelope. The adapter
    must still classify that as terminal (ContractSessionLimitError), not a
    recoverable ModelRetry — otherwise the model loops against an exhausted
    budget. Regression guard for the BLOCKED-prefix over-classification bug."""
    session = ContractSession(contract)
    for _ in range(4):  # exceed max_retries=3
        session.record_retry()
    tools = create_pydantic_ai_tools(
        contract,
        adapter=adapter,
        semantic_source=semantic,
        session=session,
        apply_middleware=False,
    )
    run_query = next(t for t in tools if t.name == "run_query")
    with pytest.raises(ContractSessionLimitError) as exc:
        await _invoke(
            run_query,
            sql="SELECT id FROM analytics.orders WHERE tenant_id = 'acme'",
        )
    assert not isinstance(exc.value, ModelRetry)
    assert "session limit exceeded" in str(exc.value).lower()


# ─── credential-free end-to-end smoke (TestModel) ─────────────────────────────


@pytest.mark.asyncio
async def test_agent_constructs_and_runs_with_tools(
    contract_no_source: DataContract, adapter: DuckDBAdapter
) -> None:
    """Registering the tools on a real Agent (with the offline TestModel)
    proves the Tool.from_schema registration is well-formed end-to-end.
    call_tools=[] keeps the run deterministic (no tool execution)."""
    tools = create_pydantic_ai_tools(contract_no_source, adapter=adapter)
    agent = Agent(model=TestModel(call_tools=[]), tools=tools)
    result = await agent.run("hello")
    assert result.output is not None


@pytest.mark.asyncio
async def test_tool_invoked_through_agent_real_path(
    contract_no_source: DataContract, adapter: DuckDBAdapter
) -> None:
    """Drive a tool through Pydantic AI's real invocation machinery
    (``function_schema`` arg-binding → ``_fn``), not just a direct ``_fn``
    call, so a schema/arg-shape regression at the Tool boundary is caught.
    TestModel synthesizes args from the tool's JSON schema; describe_table on
    a synthesized (non-allowed) table returns a benign message — no raise."""
    tools = create_pydantic_ai_tools(contract_no_source, adapter=adapter)
    agent = Agent(model=TestModel(call_tools=["describe_table"]), tools=tools)
    result = await agent.run("describe a table")
    # The tool was actually invoked through the real path (vs. never called).
    assert "describe_table" in str(result.all_messages())


# ─── deps-aware toolset (one shared Agent, per-user state) ────────────────────


def _principal_scoped_contract() -> DataContract:
    """Contract whose only table is restricted to principal 'bob'."""
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate(
                    {
                        "schema": "analytics",
                        "tables": ["orders"],
                        "allowed_principals": ["bob"],
                    }
                ),
            ],
        ),
    )
    return DataContract(schema)


@pytest.mark.asyncio
async def test_caller_principal_passthrough_gates_per_principal(
    adapter: DuckDBAdapter,
) -> None:
    """create_pydantic_ai_tools now threads caller_principal into create_tools,
    so per-principal table gating applies in the baked-in path too."""
    contract = _principal_scoped_contract()
    bob_describe = next(
        t
        for t in create_pydantic_ai_tools(
            contract, adapter=adapter, caller_principal="bob"
        )
        if t.name == "describe_table"
    )
    alice_describe = next(
        t
        for t in create_pydantic_ai_tools(
            contract, adapter=adapter, caller_principal="alice"
        )
        if t.name == "describe_table"
    )
    assert "restricted" not in await _invoke(
        bob_describe, schema="analytics", table="orders"
    )
    assert "restricted" in await _invoke(
        alice_describe, schema="analytics", table="orders"
    )


def test_create_pydantic_ai_toolset_returns_registrable_factory(
    contract_no_source: DataContract, adapter: DuckDBAdapter
) -> None:
    """The factory is a ToolsetFunc registrable on a shared Agent via the
    public agent.toolset(...) API, and builds the 9 contract tools from deps."""
    factory = create_pydantic_ai_toolset(contract_no_source, adapter=adapter)
    assert callable(factory)
    agent = Agent(model=TestModel(call_tools=[]), deps_type=ContractDeps)
    agent.toolset(factory)  # registration via the public API must not raise
    tools = _toolset_tools(
        factory, ContractDeps(session=ContractSession(contract_no_source))
    )
    assert len(tools) == 9
    assert {"run_query", "describe_table", "inspect_query"} <= set(tools)


@pytest.mark.asyncio
async def test_toolset_isolates_sessions_across_users(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    """One shared factory, two users with distinct sessions: user A exhausting
    their budget must not affect user B — the headline multi-user property."""
    factory = create_pydantic_ai_toolset(
        contract, adapter=adapter, semantic_source=semantic
    )
    session_a = ContractSession(contract)
    for _ in range(4):  # exhaust A (max_retries=3)
        session_a.record_retry()
    session_b = ContractSession(contract)  # fresh

    tools_a = _toolset_tools(factory, ContractDeps(session=session_a))
    tools_b = _toolset_tools(factory, ContractDeps(session=session_b))

    with pytest.raises(ContractSessionLimitError):
        await _invoke(tools_a["describe_table"], schema="analytics", table="orders")
    result_b = await _invoke(
        tools_b["describe_table"], schema="analytics", table="orders"
    )
    assert "BLOCKED" not in result_b


@pytest.mark.asyncio
async def test_toolset_isolation_end_to_end_through_shared_agent(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    """Drive the toolset through the REAL framework path: register the factory on
    ONE shared Agent and run two users via ``agent.run()``. This exercises
    ``agent.toolset(...)`` registration, per-run ``RunContext.deps`` threading,
    and tool dispatch — none of which the direct ``_toolset_tools`` tests cover.
    User A (exhausted) must raise the terminal error out of ``agent.run()`` while
    user B (fresh) completes on the same agent — proving real cross-user isolation."""
    factory = create_pydantic_ai_toolset(
        contract, adapter=adapter, semantic_source=semantic
    )
    agent = Agent(
        model=TestModel(call_tools=["describe_table"]), deps_type=ContractDeps
    )
    # per_run_step=False: deps (session/principal) are stable within a run, so the
    # tools need building only once per run, not once per model step. The
    # decorator-factory form is the typed public API for passing per_run_step.
    agent.toolset(per_run_step=False)(factory)

    session_b = ContractSession(contract)  # fresh user B
    result_b = await agent.run("describe orders", deps=ContractDeps(session=session_b))
    assert "describe_table" in str(result_b.all_messages())

    session_a = ContractSession(contract)  # exhausted user A
    for _ in range(4):  # exceed max_retries=3
        session_a.record_retry()
    with pytest.raises(ContractSessionLimitError):
        await agent.run("describe orders", deps=ContractDeps(session=session_a))


@pytest.mark.asyncio
async def test_toolset_applies_per_principal_gating_via_deps(
    adapter: DuckDBAdapter,
) -> None:
    """The per-user principal in deps drives per-principal table gating."""
    contract = _principal_scoped_contract()
    factory = create_pydantic_ai_toolset(contract, adapter=adapter)
    bob = _toolset_tools(
        factory, ContractDeps(session=ContractSession(contract), caller_principal="bob")
    )
    alice = _toolset_tools(
        factory,
        ContractDeps(session=ContractSession(contract), caller_principal="alice"),
    )
    assert "restricted" not in await _invoke(
        bob["describe_table"], schema="analytics", table="orders"
    )
    assert "restricted" in await _invoke(
        alice["describe_table"], schema="analytics", table="orders"
    )


@pytest.mark.asyncio
async def test_toolset_enforces_blocked_sql_via_deps(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    """Enforcement still fires through the deps-aware path: blocked SQL → ModelRetry."""
    factory = create_pydantic_ai_toolset(
        contract, adapter=adapter, semantic_source=semantic
    )
    tools = _toolset_tools(factory, ContractDeps(session=ContractSession(contract)))
    with pytest.raises(ModelRetry):
        await _invoke(tools["run_query"], sql="DELETE FROM analytics.orders")


def test_toolset_rejects_non_contract_deps(
    contract_no_source: DataContract, adapter: DuckDBAdapter
) -> None:
    """A non-ContractDeps object (None, dict, ...) hits the TypeError guard —
    fail loudly rather than silently skipping enforcement."""
    factory = create_pydantic_ai_toolset(contract_no_source, adapter=adapter)
    with pytest.raises(TypeError):
        factory(_run_ctx(None))
    with pytest.raises(TypeError):
        factory(_run_ctx({"session": None}))


def test_toolset_rejects_contract_deps_with_no_session(
    contract_no_source: DataContract, adapter: DuckDBAdapter
) -> None:
    """A ContractDeps carrying session=None hits the ValueError guard — without
    it, a None session would flow into create_pydantic_ai_tools and silently
    auto-create a fresh unbounded session, defeating enforcement."""
    factory = create_pydantic_ai_toolset(contract_no_source, adapter=adapter)
    with pytest.raises(ValueError, match="session"):
        factory(_run_ctx(ContractDeps(session=None)))  # ty: ignore[invalid-argument-type]


# ─── top-level lazy re-export ─────────────────────────────────────────────────


def test_top_level_import_resolves_when_extra_installed() -> None:
    from agentic_data_contracts import ContractDeps as _CD
    from agentic_data_contracts import create_pydantic_ai_tools as _ct
    from agentic_data_contracts import create_pydantic_ai_toolset as _cts

    assert _ct is not None
    assert _cts is not None
    assert _CD is not None
