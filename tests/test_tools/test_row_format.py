"""Row-encoding tests for run_query / preview_table (issue #44)."""

import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.semantic.yaml_source import YamlSource
from agentic_data_contracts.tools.factory import (
    _COMPACT_ROWS_NOTE,
    _render_rows,
    create_tools,
)

COLUMNS = ["region", "units", "note"]
ROWS = [
    ("EMEA", 412, None),
    ("APAC", 87, "re-run\tpending\nline2"),
    ("AMER", 0, ""),
]


class _DriverRow:
    """A row that is iterable and indexable but is neither list nor tuple.

    Stands in for a third-party adapter returning its driver's row type.
    ``dict(zip(...))`` accepts this; ``json.dumps`` does not.
    """

    def __init__(self, *values: object) -> None:
        self._values = list(values)

    def __iter__(self):
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def __getitem__(self, index: int) -> object:
        return self._values[index]


def test_compact_renders_positional_arrays() -> None:
    assert _render_rows(COLUMNS, ROWS, "compact") == [
        ["EMEA", 412, None],
        ["APAC", 87, "re-run\tpending\nline2"],
        ["AMER", 0, ""],
    ]


def test_records_renders_one_dict_per_row() -> None:
    assert _render_rows(COLUMNS, ROWS, "records")[0] == {
        "region": "EMEA",
        "units": 412,
        "note": None,
    }


def test_empty_rows_render_empty_in_both_modes() -> None:
    assert _render_rows(COLUMNS, [], "compact") == []
    assert _render_rows(COLUMNS, [], "records") == []


def test_null_stays_distinct_from_empty_string() -> None:
    rendered = json.loads(
        json.dumps(_render_rows(COLUMNS, ROWS, "compact"), default=str)
    )
    assert rendered[0][2] is None
    assert rendered[2][2] == ""


def test_tab_and_newline_survive_serialization() -> None:
    rendered = json.loads(
        json.dumps(_render_rows(COLUMNS, ROWS, "compact"), default=str)
    )
    assert rendered[1][2] == "re-run\tpending\nline2"


def test_decimal_and_date_coerce_identically_in_both_modes() -> None:
    columns = ["salary", "hired"]
    rows = [(Decimal("100000.00"), date(2025, 1, 31))]
    compact = json.loads(
        json.dumps(_render_rows(columns, rows, "compact"), default=str)
    )
    records = json.loads(
        json.dumps(_render_rows(columns, rows, "records"), default=str)
    )
    assert compact[0] == ["100000.00", "2025-01-31"]
    assert records[0] == {"salary": "100000.00", "hired": "2025-01-31"}


def test_non_tuple_row_serializes_as_array_not_string() -> None:
    # Without the list(row) coercion json.dumps routes _DriverRow through
    # default=str and emits "<_DriverRow object at 0x...>" instead of an array.
    rendered = json.loads(
        json.dumps(
            _render_rows(COLUMNS, [_DriverRow("EMEA", 412, None)], "compact"),
            default=str,
        )
    )
    assert rendered == [["EMEA", 412, None]]


def test_row_format_is_exported_from_package_root() -> None:
    import agentic_data_contracts

    assert "RowFormat" in agentic_data_contracts.__all__


SQL = "SELECT id, amount FROM analytics.orders WHERE tenant_id = 'acme'"


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
        INSERT INTO analytics.orders VALUES (1, 100.00, 'acme'), (2, 200.00, 'acme');
        CREATE TABLE analytics.customers (id INTEGER, name VARCHAR, tenant_id VARCHAR);
        CREATE TABLE analytics.subscriptions (
            id INTEGER, plan VARCHAR, tenant_id VARCHAR
        );
        """
    )
    return db


@pytest.fixture
def semantic(fixtures_dir: Path) -> YamlSource:
    return YamlSource(fixtures_dir / "semantic_source.yml")


def _tool(tools: list, name: str):
    return next(t for t in tools if t.name == name)


def test_unknown_row_format_raises_at_create_time(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    with pytest.raises(ValueError, match="row_format must be one of"):
        create_tools(
            contract,
            adapter=adapter,
            row_format="bogus",  # ty: ignore[invalid-argument-type]
        )


@pytest.mark.asyncio
async def test_run_query_compact_is_the_default(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    result = await _tool(tools, "run_query").callable({"sql": SQL})
    body = json.loads(result["content"][0]["text"])
    assert body["columns"] == ["id", "amount"]
    assert body["rows"] == [[1, "100.00"], [2, "200.00"]]


@pytest.mark.asyncio
async def test_run_query_records_reproduces_legacy_shape(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(
        contract, adapter=adapter, semantic_source=semantic, row_format="records"
    )
    result = await _tool(tools, "run_query").callable({"sql": SQL})
    body = json.loads(result["content"][0]["text"])
    assert body["rows"] == [
        {"id": 1, "amount": "100.00"},
        {"id": 2, "amount": "200.00"},
    ]


@pytest.mark.asyncio
async def test_both_modes_agree_column_for_column(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    compact = json.loads(
        (
            await _tool(
                create_tools(contract, adapter=adapter, semantic_source=semantic),
                "run_query",
            ).callable({"sql": SQL})
        )["content"][0]["text"]
    )
    records = json.loads(
        (
            await _tool(
                create_tools(
                    contract,
                    adapter=adapter,
                    semantic_source=semantic,
                    row_format="records",
                ),
                "run_query",
            ).callable({"sql": SQL})
        )["content"][0]["text"]
    )
    rebuilt = [dict(zip(compact["columns"], row)) for row in compact["rows"]]
    assert rebuilt == records["rows"]


def test_run_query_description_documents_compact_shape(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    compact = _tool(
        create_tools(contract, adapter=adapter, semantic_source=semantic), "run_query"
    )
    records = _tool(
        create_tools(
            contract, adapter=adapter, semantic_source=semantic, row_format="records"
        ),
        "run_query",
    )
    assert records.description == (
        "Validate and execute a SQL query, returning the results."
    )
    assert compact.description == records.description + _COMPACT_ROWS_NOTE


@pytest.mark.asyncio
async def test_preview_table_compact_carries_columns(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    result = await _tool(tools, "preview_table").callable(
        {"schema": "analytics", "table": "orders"}
    )
    text = result["content"][0]["text"]
    # `columns` must precede `rows` in the serialized body — json.dumps
    # preserves insertion order, so the model reads the header before the
    # positional values it must align to (factory.py:449-450).
    assert text.index('"columns"') < text.index('"rows"')
    body = json.loads(text)
    assert body["columns"] == ["id", "amount", "tenant_id"]
    assert body["rows"][0] == [1, "100.00", "acme"]


@pytest.mark.asyncio
async def test_preview_table_records_also_carries_columns(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(
        contract, adapter=adapter, semantic_source=semantic, row_format="records"
    )
    result = await _tool(tools, "preview_table").callable(
        {"schema": "analytics", "table": "orders"}
    )
    body = json.loads(result["content"][0]["text"])
    assert body["columns"] == ["id", "amount", "tenant_id"]
    assert body["rows"][0] == {"id": 1, "amount": "100.00", "tenant_id": "acme"}


@pytest.mark.asyncio
async def test_zero_row_preview_still_reports_columns(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    # The gap this closes: an empty preview used to return {"rows": []} and
    # tell the agent nothing about the table's shape.
    adapter.connection.execute("DELETE FROM analytics.orders")
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    result = await _tool(tools, "preview_table").callable(
        {"schema": "analytics", "table": "orders"}
    )
    body = json.loads(result["content"][0]["text"])
    assert body["rows"] == []
    assert body["columns"] == ["id", "amount", "tenant_id"]


def test_preview_table_description_documents_compact_shape(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    compact = _tool(
        create_tools(contract, adapter=adapter, semantic_source=semantic),
        "preview_table",
    )
    records = _tool(
        create_tools(
            contract, adapter=adapter, semantic_source=semantic, row_format="records"
        ),
        "preview_table",
    )
    assert records.description == "Preview sample rows from an allowed table."
    assert compact.description == records.description + _COMPACT_ROWS_NOTE


def test_pydantic_ai_toolset_validates_row_format_eagerly(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    # The toolset builds its tools inside a per-run factory, so without its own
    # check a typo would surface on the first agent run instead of at wiring.
    pytest.importorskip("pydantic_ai")
    from agentic_data_contracts.tools.pydantic_ai import create_pydantic_ai_toolset

    with pytest.raises(ValueError, match="row_format must be one of"):
        create_pydantic_ai_toolset(
            contract,
            adapter=adapter,
            row_format="bogus",  # ty: ignore[invalid-argument-type]
        )


def test_langchain_wrapper_accepts_row_format(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    pytest.importorskip("langchain_core")
    from agentic_data_contracts.tools.langchain import create_langchain_tools

    tools = create_langchain_tools(
        contract, adapter=adapter, semantic_source=semantic, row_format="records"
    )
    preview = next(t for t in tools if t.name == "preview_table")
    assert "positionally aligned" not in preview.description


def test_sdk_wrapper_forwards_row_format_to_create_tools(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    # Proves the value actually reaches create_tools (the only place that
    # raises this ValueError) rather than merely appearing in the wrapper's
    # signature — a dropped `row_format=row_format` forward at sdk.py would
    # let a bogus value through silently and this test would still pass if it
    # only inspected the signature. `tools=` is deliberately omitted so the
    # `if tools is None:` branch in create_sdk_mcp_server runs and calls
    # create_tools itself.
    pytest.importorskip("claude_agent_sdk")
    from agentic_data_contracts.tools.sdk import create_sdk_mcp_server

    with pytest.raises(ValueError, match="row_format must be one of"):
        create_sdk_mcp_server(
            contract,
            adapter=adapter,
            row_format="bogus",  # ty: ignore[invalid-argument-type]
        )


def test_prebuilt_tools_list_takes_precedence_over_row_format(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    pytest.importorskip("langchain_core")
    from agentic_data_contracts.tools.langchain import create_langchain_tools

    prebuilt = create_tools(
        contract, adapter=adapter, semantic_source=semantic, row_format="compact"
    )
    tools = create_langchain_tools(
        contract,
        adapter=adapter,
        semantic_source=semantic,
        tools=prebuilt,
        row_format="records",
    )
    preview = next(t for t in tools if t.name == "preview_table")
    assert "positionally aligned" in preview.description


def test_pydantic_ai_tools_forwards_row_format_to_create_tools(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    # Mirrors test_sdk_wrapper_forwards_row_format_to_create_tools above:
    # proves row_format actually reaches create_tools (the only place that
    # raises this ValueError) rather than merely appearing in the wrapper's
    # signature. `tools=` is deliberately omitted so the `if tools is None:`
    # branch in create_pydantic_ai_tools runs and calls create_tools itself.
    pytest.importorskip("pydantic_ai")
    from agentic_data_contracts.tools.pydantic_ai import create_pydantic_ai_tools

    with pytest.raises(ValueError, match="row_format must be one of"):
        create_pydantic_ai_tools(
            contract,
            adapter=adapter,
            row_format="bogus",  # ty: ignore[invalid-argument-type]
        )


def test_pydantic_ai_toolset_factory_forwards_row_format(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    # test_pydantic_ai_toolset_validates_row_format_eagerly (above) only
    # proves the toolset's own eager pre-check at wiring time — that check
    # runs before _factory is ever invoked, so it never touches the
    # `row_format=row_format` forward inside _factory's create_pydantic_ai_tools
    # call. Actually invoke the factory with a real ContractDeps/RunContext so
    # a dropped forward there is caught too.
    pytest.importorskip("pydantic_ai")
    from typing import Any, cast

    from pydantic_ai import RunContext
    from pydantic_ai.models.test import TestModel
    from pydantic_ai.usage import RunUsage

    from agentic_data_contracts.core.session import ContractSession
    from agentic_data_contracts.tools.pydantic_ai import (
        ContractDeps,
        create_pydantic_ai_toolset,
    )

    factory = create_pydantic_ai_toolset(
        contract, adapter=adapter, semantic_source=semantic, row_format="records"
    )
    deps = ContractDeps(session=ContractSession(contract))
    ctx = RunContext(deps=deps, model=TestModel(), usage=RunUsage())
    # create_pydantic_ai_toolset's declared return type is a ToolsetFunc whose
    # call signature covers async-factory and None cases the deps-aware path
    # never takes; cast through Any here, same as test_pydantic_ai.py's
    # _toolset_tools helper, rather than a line-level ty:ignore.
    toolset = cast(Any, factory(ctx))
    preview = toolset.tools["preview_table"]
    # "positionally aligned" is appended only in compact mode (factory.py's
    # _COMPACT_ROWS_NOTE); its absence proves "records" travelled through
    # _factory's create_pydantic_ai_tools(..., row_format=row_format) call
    # into create_tools, not just into the toolset factory's own signature.
    assert "positionally aligned" not in preview.description
