"""Tests for the Pydantic AI Tool adapter."""

from pathlib import Path
from typing import Any, cast

import pytest

# Skip the entire module if the optional extra isn't installed —
# matches the "extra is optional" backward-compat contract.
pytest.importorskip("pydantic_ai")

from pydantic_ai import Agent, ModelRetry, RunContext, Tool  # noqa: E402
from pydantic_ai.models.test import TestModel  # noqa: E402
from pydantic_ai.usage import RunUsage, UsageLimits  # noqa: E402

from agentic_data_contracts.adapters.duckdb import DuckDBAdapter  # noqa: E402
from agentic_data_contracts.core.contract import DataContract  # noqa: E402
from agentic_data_contracts.core.schema import (  # noqa: E402
    AllowedTable,
    DataContractSchema,
    ResourceConfig,
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
    usage_limits_from_contract,
)

# ─── helpers ──────────────────────────────────────────────────────────────────


async def _invoke(
    tool: Tool, *, ctx: RunContext[Any] | None = None, **kwargs: Any
) -> Any:
    """Call a wrapped tool's underlying function with keyword args.

    The wrapper is registered ``takes_ctx=True`` — it reads ``ctx.usage`` to
    feed the contract's token budget — so a ``RunContext`` is required.
    Tests that do not exercise usage get a fresh zero-usage one; pass ``ctx``
    to drive it. Going through an ``Any`` indirection lets tests invoke the
    function directly without the type checker flagging the call shape.
    """
    fn = cast(Any, tool.function)
    return await fn(ctx if ctx is not None else _run_ctx(None), **kwargs)


def _run_ctx(deps: Any) -> RunContext[Any]:
    """Minimal RunContext for driving a deps-aware toolset factory directly."""
    return RunContext(deps=deps, model=TestModel(), usage=RunUsage(), run_id="test-run")


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
    from agentic_data_contracts import usage_limits_from_contract as _ul

    assert _ct is not None
    assert _cts is not None
    assert _CD is not None
    assert _ul is not None


# ─── token budget (v0.34.0) ───────────────────────────────────────────────────


def _ctx_with_usage(total: int, run_id: str = "run-1") -> RunContext[Any]:
    """A RunContext reporting `total` cumulative tokens for `run_id`."""
    return RunContext(
        deps=None,
        model=TestModel(),
        usage=RunUsage(input_tokens=total),
        run_id=run_id,
    )


@pytest.mark.asyncio
async def test_tool_call_feeds_the_token_budget(
    contract_no_source: DataContract, adapter: DuckDBAdapter
) -> None:
    # Before this, ContractSession.record_tokens was called only from tests, so
    # a declared token_budget was inert and remaining() reported the full
    # budget forever.
    session = ContractSession(contract_no_source)
    tools = {
        t.name: t
        for t in create_pydantic_ai_tools(
            contract_no_source, adapter=adapter, session=session
        )
    }
    await _invoke(
        tools["describe_table"],
        ctx=_ctx_with_usage(1_500),
        schema="analytics",
        table="orders",
    )
    assert session.tokens_used == 1_500


@pytest.mark.asyncio
async def test_repeated_calls_in_one_run_do_not_multiply(
    contract_no_source: DataContract, adapter: DuckDBAdapter
) -> None:
    # ctx.usage is cumulative for the run, so adding it on each call would
    # count 1500 + 2200 = 3700 instead of the true 2200.
    session = ContractSession(contract_no_source)
    tools = {
        t.name: t
        for t in create_pydantic_ai_tools(
            contract_no_source, adapter=adapter, session=session
        )
    }
    for total in (1_500, 2_200):
        await _invoke(
            tools["describe_table"],
            ctx=_ctx_with_usage(total),
            schema="analytics",
            table="orders",
        )
    assert session.tokens_used == 2_200


@pytest.mark.asyncio
async def test_usage_accumulates_across_runs_on_one_session(
    contract_no_source: DataContract, adapter: DuckDBAdapter
) -> None:
    # The ContractDeps pattern: one session per user, reused every turn. Each
    # run's counter restarts, so the session must total them, not reset.
    session = ContractSession(contract_no_source)
    tools = {
        t.name: t
        for t in create_pydantic_ai_tools(
            contract_no_source, adapter=adapter, session=session
        )
    }
    for run_id, total in (("run-1", 1_000), ("run-2", 700)):
        await _invoke(
            tools["describe_table"],
            ctx=_ctx_with_usage(total, run_id=run_id),
            schema="analytics",
            table="orders",
        )
    assert session.tokens_used == 1_700


@pytest.mark.asyncio
async def test_exhausted_token_budget_is_terminal(adapter: DuckDBAdapter) -> None:
    # A budget breach cannot be fixed by retrying, so it must raise the
    # terminal error rather than a recoverable ModelRetry.
    contract = DataContract(
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
    session = ContractSession(contract)
    tools = {
        t.name: t
        for t in create_pydantic_ai_tools(contract, adapter=adapter, session=session)
    }
    with pytest.raises(ContractSessionLimitError, match="token"):
        await _invoke(
            tools["describe_table"],
            ctx=_ctx_with_usage(50_001),
            schema="analytics",
            table="orders",
        )


# ─── usage_limits_from_contract (v0.36.0) ─────────────────────────────────────


def _budgeted_contract(budget: int | None = 50_000) -> DataContract:
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
            resources=(
                ResourceConfig(token_budget=budget) if budget is not None else None
            ),
        )
    )


def test_usage_limits_maps_token_budget() -> None:
    session = ContractSession(_budgeted_contract())
    assert (
        usage_limits_from_contract(_budgeted_contract(), session).total_tokens_limit
        == 50_000
    )


def test_usage_limits_subtracts_what_the_session_already_spent() -> None:
    """The whole reason the helper takes a session.

    ``UsageLimits`` is checked against ``RunUsage`` — per ``agent.run()`` call —
    while a ``ContractSession``'s budget is per *user across every turn*
    (``ContractDeps`` says to reuse one session so limits accumulate). Mapping
    the raw budget would therefore grant it afresh on every turn: a 50k
    contract would authorise 50k *per turn*, which is not what it declares.
    """
    contract = _budgeted_contract()
    session = ContractSession(contract)
    session.observe_tokens(30_000, scope="turn-1")

    limits = usage_limits_from_contract(contract, session)
    assert limits.total_tokens_limit == 20_000


def test_usage_limits_floor_at_zero_never_goes_negative() -> None:
    # A negative limit would be nonsense to pydantic-ai; an exhausted budget
    # must clamp to 0, which aborts the run on its first usage check.
    contract = _budgeted_contract()
    session = ContractSession(contract)
    session.observe_tokens(70_000, scope="over")

    assert usage_limits_from_contract(contract, session).total_tokens_limit == 0


def test_usage_limits_distinguishes_a_zero_budget_from_no_budget() -> None:
    """``token_budget: 0`` is a declaration, not an absence.

    Zero means "spend nothing"; absent means "no ceiling". Collapsing them
    would turn the strictest possible budget into no budget at all, which is
    the same declared-but-unenforced shape this helper exists to avoid.
    """
    contract = _budgeted_contract(0)
    assert (
        usage_limits_from_contract(
            contract, ContractSession(contract)
        ).total_tokens_limit
        == 0
    )


def test_usage_limits_leaves_token_limit_unset_without_a_budget() -> None:
    # No declared budget means no token ceiling to translate. Inventing one
    # would be the mirror image of the declared-but-unenforced bug.
    contract = _budgeted_contract(budget=None)
    limits = usage_limits_from_contract(contract, ContractSession(contract))
    assert limits.total_tokens_limit is None


def test_usage_limits_does_not_map_max_retries_onto_request_limit() -> None:
    """The trap this helper exists partly to prevent.

    ``max_retries`` counts *blocked query attempts* in this library;
    ``request_limit`` counts *model requests*. A contract saying
    ``max_retries: 3`` means "three bad queries", not "three LLM calls" —
    mapping one onto the other silently changes what an existing contract
    means. ``request_limit`` is therefore left at pydantic-ai's own default.
    """
    contract = DataContract(
        DataContractSchema(
            name="retries",
            semantic=SemanticConfig(
                allowed_tables=[
                    AllowedTable.model_validate(
                        {"schema": "analytics", "tables": ["orders"]}
                    ),
                ],
            ),
            resources=ResourceConfig(max_retries=3, token_budget=50_000),
        )
    )
    limits = usage_limits_from_contract(contract, ContractSession(contract))
    assert limits.request_limit == UsageLimits().request_limit
    assert limits.request_limit != 3


@pytest.mark.asyncio
async def test_multi_turn_spend_is_bounded_but_not_exact(
    adapter: DuckDBAdapter,
) -> None:
    """The honest property, measured through the wiring users actually write.

    An earlier version of this test asserted that spending exactly what the
    helper allows leaves the session exactly at its budget. That was arithmetic
    dressed up as a guarantee: it *stipulated* the session observes the full
    run, which is the one thing that does not happen. The session is fed only
    inside the tool wrapper, so every model request after a run's last tool
    call — including answer generation — goes unobserved, and the subtraction
    compounds that shortfall each turn.

    So the real property is a bound, not an identity: the framework does stop
    the agent, at a multiple of the budget rather than at it.

    The regression this guards is the unsubtracted mapping from issue #53. Note
    *how* that one dies here — not by running away until the loop gives up, but
    by the session's own in-tool enforcement raising ``ContractSessionLimitError``
    on the third turn, because the per-run limit stops constraining anything and
    the tools hit the ceiling first. That is caught explicitly below rather than
    left to propagate, so the failure names the cause instead of surfacing as an
    unrelated error.
    """
    from pydantic_ai.exceptions import UsageLimitExceeded
    from pydantic_ai.messages import ModelResponse, TextPart, ToolCallPart
    from pydantic_ai.models.function import AgentInfo, FunctionModel
    from pydantic_ai.usage import RequestUsage

    budget = 500
    per_request = 100
    contract = _budgeted_contract(budget)
    spent = 0

    def _model(messages: list[Any], info: AgentInfo) -> ModelResponse:
        # One tool call, then a text answer — the shape that leaves an
        # unobserved tail on every turn.
        nonlocal spent
        spent += per_request
        usage = RequestUsage(
            input_tokens=per_request // 2, output_tokens=per_request // 2
        )
        called = any(
            getattr(part, "part_kind", "") == "tool-return"
            for message in messages
            for part in getattr(message, "parts", [])
        )
        if called:
            return ModelResponse(parts=[TextPart("done")], usage=usage)
        return ModelResponse(
            parts=[
                ToolCallPart(
                    "describe_table", {"schema": "analytics", "table": "orders"}
                )
            ],
            usage=usage,
        )

    session = ContractSession(contract)
    agent = Agent(
        FunctionModel(_model),
        tools=create_pydantic_ai_tools(contract, adapter=adapter, session=session),
    )

    stopped = False
    for turn in range(12):
        try:
            await agent.run(
                "go", usage_limits=usage_limits_from_contract(contract, session)
            )
        except UsageLimitExceeded:
            stopped = True
            break
        except ContractSessionLimitError as e:  # pragma: no cover — regression guard
            pytest.fail(
                f"turn {turn}: the session enforcer fired before the framework"
                f" did, which means the per-run limit stopped constraining the"
                f" run — the unsubtracted mapping from issue #53. ({e})"
            )

    assert stopped, "the framework must eventually refuse the run"
    # The framework fires while spend is still a small multiple of the budget.
    # Loose because the overshoot tracks the tool-call-to-request ratio, not a
    # constant — see the module's known limitations.
    assert spent <= budget * 3


@pytest.mark.asyncio
async def test_limits_actually_stop_a_real_run() -> None:
    """The load-bearing test: does the helper's output bind through agent.run()?

    Every other test in this section checks arithmetic on the returned object,
    and all of them would still pass if ``UsageLimits`` were ignored entirely —
    which is the whole feature. This drives a real ``Agent.run`` with a budget
    the session has all but spent, and asserts the run is refused.

    That refusal arrives from Pydantic AI on a *model request*, with no tool
    call needed — which is precisely the window ``check_limits()`` cannot
    close, since it only ever runs when a tool is about to be invoked.
    """
    from pydantic_ai.exceptions import UsageLimitExceeded

    contract = _budgeted_contract()
    session = ContractSession(contract)
    session.observe_tokens(49_999, scope="earlier-turns")

    agent = Agent(TestModel())
    with pytest.raises(UsageLimitExceeded):
        await agent.run(
            "anything",
            usage_limits=usage_limits_from_contract(contract, session),
        )


@pytest.mark.asyncio
async def test_a_fresh_budget_does_not_stop_a_run() -> None:
    # The other half: the limit must not be so tight that it refuses everything.
    # Without this, the test above would pass on a helper that always returns 0.
    contract = _budgeted_contract()
    session = ContractSession(contract)

    result = await Agent(TestModel()).run(
        "anything",
        usage_limits=usage_limits_from_contract(contract, session),
    )
    assert result.output
