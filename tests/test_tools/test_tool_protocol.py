"""Protocol enforcement carried by the tool descriptions themselves.

The same workflow guidance also appears in ``ClaudePromptRenderer`` output, but
that surface is opt-in: a host wiring ``create_langchain_tools`` or
``create_pydantic_ai_tools`` into its own agent supplies its own system prompt
and may never render the contract at all. Descriptions travel with the tools on
every path, so the ordering and precedence rules are asserted here — including
across the ecosystem wrappers, since "descriptions travel on every path" is the
premise the whole feature rests on.

Both clauses obey one rule: a clause appears only when the capability it names
exists. Ordering needs metrics to look up; precedence needs an adapter that can
actually execute.
"""

from pathlib import Path

import pytest

from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.semantic.yaml_source import YamlSource
from agentic_data_contracts.tools.factory import (
    _COMPACT_ROWS_NOTE,
    _PROTOCOL_METRIC_ORDERING,
    _PROTOCOL_PRECEDENCE,
    ToolDef,
    create_tools,
)


@pytest.fixture
def contract(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml(fixtures_dir / "valid_contract.yml")


@pytest.fixture
def sourceless_contract(fixtures_dir: Path) -> DataContract:
    # No `semantic.source` at all, so load_semantic_source() returns None.
    return DataContract.from_yaml(fixtures_dir / "minimal_contract.yml")


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
        """
    )
    return db


def _tool(tools: list[ToolDef], name: str) -> ToolDef:
    return next(t for t in tools if t.name == name)


# ── Ordering clause (gated on metrics existing) ──────────────────────────────


def test_ordering_present_when_metrics_exist(
    contract: DataContract, semantic: YamlSource, adapter: DuckDBAdapter
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    for name in ("run_query", "inspect_query"):
        assert _PROTOCOL_METRIC_ORDERING in _tool(tools, name).description


def test_ordering_omitted_without_semantic_source(
    sourceless_contract: DataContract, adapter: DuckDBAdapter
) -> None:
    tools = create_tools(sourceless_contract, adapter=adapter)
    for name in ("run_query", "inspect_query"):
        assert _PROTOCOL_METRIC_ORDERING not in _tool(tools, name).description


def test_ordering_omitted_when_source_has_no_metrics(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    # Distinct code path from "no source at all": a source object exists and is
    # queried, it just yields nothing to look up.
    tools = create_tools(
        contract, adapter=adapter, semantic_source=YamlSource.from_raw({})
    )
    for name in ("run_query", "inspect_query"):
        assert _PROTOCOL_METRIC_ORDERING not in _tool(tools, name).description


# ── Precedence clause (gated on an adapter that can execute) ─────────────────


def test_precedence_present_whenever_an_adapter_can_execute(
    contract: DataContract,
    sourceless_contract: DataContract,
    semantic: YamlSource,
    adapter: DuckDBAdapter,
) -> None:
    # Independent of the semantic layer — unlike ordering, it has no metric
    # precondition.
    for tools in (
        create_tools(contract, adapter=adapter, semantic_source=semantic),
        create_tools(
            contract, adapter=adapter, semantic_source=YamlSource.from_raw({})
        ),
        create_tools(sourceless_contract, adapter=adapter),
    ):
        assert _PROTOCOL_PRECEDENCE in _tool(tools, "run_query").description


def test_precedence_omitted_without_an_adapter(
    contract: DataContract, semantic: YamlSource
) -> None:
    # Without an adapter run_query returns "No database adapter configured —
    # cannot execute query", so claiming precedence over other data-access paths
    # would steer the agent off a path that works and onto one that cannot run.
    run_query = _tool(create_tools(contract, semantic_source=semantic), "run_query")
    assert _PROTOCOL_PRECEDENCE not in run_query.description
    # The two gates are independent: metrics exist, so ordering still applies.
    assert _PROTOCOL_METRIC_ORDERING in run_query.description


def test_inspect_query_never_carries_precedence(
    contract: DataContract, semantic: YamlSource, adapter: DuckDBAdapter
) -> None:
    # inspect_query executes nothing, so "prefer this over other data-access
    # paths" would be a false claim there — and its description already closes
    # with its own precedence argument against spending retry budget.
    inspect = _tool(
        create_tools(contract, adapter=adapter, semantic_source=semantic),
        "inspect_query",
    )
    assert _PROTOCOL_METRIC_ORDERING in inspect.description
    assert _PROTOCOL_PRECEDENCE not in inspect.description


# ── Composition ──────────────────────────────────────────────────────────────


def test_run_query_description_composes_exactly(
    contract: DataContract, semantic: YamlSource, adapter: DuckDBAdapter
) -> None:
    # Exact equality, composed from the constants rather than restating the
    # prose: pins clause order and catches an accidental double-append that a
    # substring check would pass.
    run_query = _tool(
        create_tools(
            contract, adapter=adapter, semantic_source=semantic, row_format="records"
        ),
        "run_query",
    )
    assert run_query.description == (
        "Validate and execute a SQL query, returning the results."
        + _PROTOCOL_METRIC_ORDERING
        + _PROTOCOL_PRECEDENCE
    )


def test_other_tools_carry_no_protocol_text(
    contract: DataContract, semantic: YamlSource, adapter: DuckDBAdapter
) -> None:
    # Scope guard: the protocol belongs on the two query tools only. Every
    # description is re-sent on every model request, so drift here is billed
    # forever. Iterates whatever create_tools returns, so a future tenth tool
    # is covered without editing this test.
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    for tool in tools:
        if tool.name in {"run_query", "inspect_query"}:
            continue
        assert _PROTOCOL_METRIC_ORDERING not in tool.description
        assert _PROTOCOL_PRECEDENCE not in tool.description


def test_compact_rows_note_remains_the_description_suffix(
    contract: DataContract, semantic: YamlSource, adapter: DuckDBAdapter
) -> None:
    # The row-shape clause describes the *return value*, so it reads last —
    # after the call-time guidance.
    compact = _tool(
        create_tools(contract, adapter=adapter, semantic_source=semantic), "run_query"
    ).description
    records = _tool(
        create_tools(
            contract, adapter=adapter, semantic_source=semantic, row_format="records"
        ),
        "run_query",
    ).description
    assert compact.endswith(_COMPACT_ROWS_NOTE)
    assert compact == records + _COMPACT_ROWS_NOTE


# ── The premise: descriptions survive every wrapper conversion ───────────────
#
# Each wrapper passes ToolDef.description through verbatim today. These pin that
# it stays true: a refactor that truncated, templated, or dropped the
# description would silently void the feature's entire justification while the
# ToolDef-level tests above stayed green.


def test_langchain_wrapper_preserves_protocol(
    contract: DataContract, semantic: YamlSource, adapter: DuckDBAdapter
) -> None:
    pytest.importorskip("langchain_core")
    from agentic_data_contracts.tools.langchain import create_langchain_tools

    tools = create_langchain_tools(contract, adapter=adapter, semantic_source=semantic)
    run_query = next(t for t in tools if t.name == "run_query")
    assert _PROTOCOL_METRIC_ORDERING in run_query.description
    assert _PROTOCOL_PRECEDENCE in run_query.description


def test_pydantic_ai_wrapper_preserves_protocol(
    contract: DataContract, semantic: YamlSource, adapter: DuckDBAdapter
) -> None:
    pytest.importorskip("pydantic_ai")
    from agentic_data_contracts.tools.pydantic_ai import create_pydantic_ai_tools

    tools = create_pydantic_ai_tools(
        contract, adapter=adapter, semantic_source=semantic
    )
    run_query = next(t for t in tools if t.name == "run_query")
    description = run_query.description or ""
    assert _PROTOCOL_METRIC_ORDERING in description
    assert _PROTOCOL_PRECEDENCE in description


# No equivalent for create_sdk_mcp_server: it forwards t.description positionally
# into claude_agent_sdk's `tool` decorator, and the resulting MCP `Server` exposes
# no public read-back for a registered tool's description (only decorator-
# registered handlers). Asserting through MCP internals would couple this suite to
# a third-party private API that can change without notice, and a mock would only
# restate the call we already read. The forward is a single positional argument in
# create_sdk_mcp_server, so the drift risk it leaves uncovered is the smallest of
# the three wrappers.
