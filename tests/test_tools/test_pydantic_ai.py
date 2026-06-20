"""Tests for the Pydantic AI Tool adapter."""

from pathlib import Path
from typing import Any, cast

import pytest

# Skip the entire module if the optional extra isn't installed —
# matches the "extra is optional" backward-compat contract.
pytest.importorskip("pydantic_ai")

from pydantic_ai import Agent, ModelRetry, Tool  # noqa: E402
from pydantic_ai.models.test import TestModel  # noqa: E402

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
    _unwrap_mcp_text,
    create_pydantic_ai_tools,
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


# ─── top-level lazy re-export ─────────────────────────────────────────────────


def test_top_level_import_resolves_when_extra_installed() -> None:
    from agentic_data_contracts import create_pydantic_ai_tools as _ct

    assert _ct is not None
