"""Protocol enforcement carried by the tool descriptions themselves.

The same workflow guidance also appears in ``ClaudePromptRenderer`` output, but
that surface is opt-in: a host wiring ``create_langchain_tools`` or
``create_pydantic_ai_tools`` into its own agent supplies its own system prompt
and may never render the contract at all. Descriptions travel with the tools on
every path, so the ordering and precedence rules are asserted here.
"""

from pathlib import Path

import pytest

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


def _tool(tools: list[ToolDef], name: str) -> ToolDef:
    return next(t for t in tools if t.name == name)


def test_run_query_carries_ordering_when_metrics_exist(
    contract: DataContract, semantic: YamlSource
) -> None:
    tools = create_tools(contract, semantic_source=semantic)
    assert _PROTOCOL_METRIC_ORDERING in _tool(tools, "run_query").description


def test_run_query_omits_ordering_without_semantic_source(
    sourceless_contract: DataContract,
) -> None:
    tools = create_tools(sourceless_contract)
    assert _PROTOCOL_METRIC_ORDERING not in _tool(tools, "run_query").description


def test_run_query_omits_ordering_when_source_has_no_metrics(
    contract: DataContract,
) -> None:
    # Distinct code path from "no source at all": a source object exists and is
    # queried, it just yields nothing to look up.
    empty = YamlSource.from_raw({})
    tools = create_tools(contract, semantic_source=empty)
    assert _PROTOCOL_METRIC_ORDERING not in _tool(tools, "run_query").description


def test_run_query_always_carries_precedence(
    contract: DataContract, sourceless_contract: DataContract, semantic: YamlSource
) -> None:
    # The precedence claim is about execution routing, which is true of every
    # contract — unlike the ordering rule it has no semantic-layer precondition.
    for tools in (
        create_tools(contract, semantic_source=semantic),
        create_tools(contract, semantic_source=YamlSource.from_raw({})),
        create_tools(sourceless_contract),
    ):
        assert _PROTOCOL_PRECEDENCE in _tool(tools, "run_query").description


def test_inspect_query_carries_ordering_but_not_precedence(
    contract: DataContract, semantic: YamlSource
) -> None:
    # inspect_query executes nothing, so "prefer this over other data-access
    # paths" would be a false claim there — and its description already closes
    # with its own precedence argument against spending retry budget.
    inspect = _tool(create_tools(contract, semantic_source=semantic), "inspect_query")
    assert _PROTOCOL_METRIC_ORDERING in inspect.description
    assert _PROTOCOL_PRECEDENCE not in inspect.description


def test_other_tools_carry_no_protocol_text(
    contract: DataContract, semantic: YamlSource
) -> None:
    # Scope guard: the protocol belongs on the two query tools only. Every
    # description is re-sent on every model request, so drift here is billed
    # forever.
    tools = create_tools(contract, semantic_source=semantic)
    for tool in tools:
        if tool.name in {"run_query", "inspect_query"}:
            continue
        assert _PROTOCOL_METRIC_ORDERING not in tool.description
        assert _PROTOCOL_PRECEDENCE not in tool.description


def test_compact_rows_note_remains_the_description_suffix(
    contract: DataContract, semantic: YamlSource
) -> None:
    # The row-shape clause describes the *return value*, so it reads last —
    # after the call-time guidance. This also keeps the compact/records
    # descriptions differing by exactly the note.
    compact = _tool(
        create_tools(contract, semantic_source=semantic), "run_query"
    ).description
    records = _tool(
        create_tools(contract, semantic_source=semantic, row_format="records"),
        "run_query",
    ).description
    assert compact.endswith(_COMPACT_ROWS_NOTE)
    assert compact == records + _COMPACT_ROWS_NOTE
