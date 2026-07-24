"""Row-encoding tests for run_query / preview_table (issue #44)."""

import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.semantic.yaml_source import YamlSource
from agentic_data_contracts.tools.factory import _render_rows, create_tools

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
        create_tools(contract, adapter=adapter, row_format="bogus")  # type: ignore


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
    assert "positionally aligned" in compact.description
    assert records.description == (
        "Validate and execute a SQL query, returning the results."
    )


@pytest.mark.asyncio
async def test_preview_table_compact_carries_columns(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    result = await _tool(tools, "preview_table").callable(
        {"schema": "analytics", "table": "orders"}
    )
    body = json.loads(result["content"][0]["text"])
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
    assert "positionally aligned" in compact.description
    assert records.description == "Preview sample rows from an allowed table."
