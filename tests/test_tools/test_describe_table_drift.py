"""``describe_table`` says when the documentation it just served is stale (#90).

The sharp part of the issue: this one function already held both sides and did
not compare them. It overlays authored descriptions by iterating the *adapter's*
columns and looking each one up in the semantic source -- a left join. An
adapter column with no declaration is fine and intended. A **declared column
with no adapter column** simply never matches, and fell out with no signal.

`check_schema_drift` is the CI half. This is the agent-facing half: a `note`,
not an error, because the response is still useful -- the live column list is
correct and the table description still applies. What changes is that the agent
now knows its documentation disagrees with the warehouse, instead of being
handed a confident answer built on a stale declaration.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.semantic.yaml_source import YamlSource
from agentic_data_contracts.tools.factory import ToolDef, create_tools


@pytest.fixture
def contract(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml(fixtures_dir / "valid_contract.yml")


@pytest.fixture
def adapter() -> DuckDBAdapter:
    db = DuckDBAdapter(":memory:")
    db.connection.execute(
        """
        CREATE SCHEMA IF NOT EXISTS analytics;
        CREATE TABLE analytics.orders (id INTEGER, amount DECIMAL(10,2));
        """
    )
    return db


def _tool(tools: list[ToolDef], name: str) -> ToolDef:
    return next(t for t in tools if t.name == name)


def _source(columns: list[dict[str, Any]]) -> YamlSource:
    return YamlSource.from_raw(
        {
            "metrics": [],
            "tables": [{"schema": "analytics", "table": "orders", "columns": columns}],
        },
        expected_extras=[],
    )


async def _describe(
    contract: DataContract,
    adapter: Any,
    source: Any,
    table: str = "orders",
) -> dict[str, Any]:
    tools = create_tools(contract, adapter=adapter, semantic_source=source)
    result = await _tool(tools, "describe_table").callable(
        {"schema": "analytics", "table": table}
    )
    return json.loads(result["content"][0]["text"])


@pytest.mark.asyncio
async def test_a_declared_column_that_does_not_exist_is_noted(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    """The reproduction from #90."""
    payload = await _describe(
        contract,
        adapter,
        _source(
            [
                {"name": "id", "description": "real column"},
                {"name": "column1", "description": "PHANTOM: no longer exists"},
            ]
        ),
    )
    assert "column1" in payload["note"]


@pytest.mark.asyncio
async def test_the_note_does_not_make_the_call_an_error(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    """The live column list is still correct and still worth having. Failing the
    call would cost the agent a usable answer over a documentation defect."""
    tools = create_tools(
        contract, adapter=adapter, semantic_source=_source([{"name": "column1"}])
    )
    result = await _tool(tools, "describe_table").callable(
        {"schema": "analytics", "table": "orders"}
    )
    assert not result.get("is_error")
    # `_kind` is set only on error envelopes; a successful call carries none.
    assert "_kind" not in result


@pytest.mark.asyncio
async def test_a_clean_declaration_adds_no_note(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    """Omitted rather than empty, like every other optional key on this payload
    -- a note saying nothing spends an agent's context to no purpose."""
    payload = await _describe(
        contract, adapter, _source([{"name": "id"}, {"name": "amount"}])
    )
    assert "note" not in payload


@pytest.mark.asyncio
async def test_an_undescribed_declaration_is_checked_too(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    """The overlay dict holds only *described* columns; diffing it would miss a
    declared column that carries no description. It is just as absent."""
    payload = await _describe(contract, adapter, _source([{"name": "column1"}]))
    assert "column1" in payload["note"]


@pytest.mark.asyncio
async def test_a_live_column_nobody_declared_is_not_noted(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    payload = await _describe(contract, adapter, _source([{"name": "id"}]))
    assert "note" not in payload
    assert "amount" in [c["name"] for c in payload["columns"]]


@pytest.mark.asyncio
async def test_no_semantic_source_adds_no_note(adapter: DuckDBAdapter) -> None:
    """`valid_contract.yml` declares a dbt source, so `semantic_source=None`
    makes the factory load *that*. This needs a contract declaring none."""
    bare = DataContract.from_yaml_string(
        """
version: "1.0"
name: no-source
semantic:
  allowed_tables:
    - schema: analytics
      tables: [orders]
"""
    )
    payload = await _describe(bare, adapter, None)
    assert "note" not in payload


@pytest.mark.asyncio
async def test_a_case_only_difference_names_the_live_spelling(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    """`ID` does not match `id` in the overlay's dict lookup, so the authored
    description silently fails to reach the agent. Telling it the column is
    missing would be wrong -- it is right there, spelled differently."""
    payload = await _describe(
        contract, adapter, _source([{"name": "ID", "description": "the key"}])
    )
    note = payload["note"]
    assert "ID" in note
    assert "id" in note
    # The description did not reach the column, which is the actual damage.
    column = next(c for c in payload["columns"] if c["name"] == "id")
    assert "description" not in column


@pytest.mark.asyncio
async def test_a_long_list_of_stale_columns_is_truncated_and_says_so(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    """A table renamed wholesale would otherwise put every declared column into
    an agent's context. Truncating without saying so is the silent drop this
    issue is about, one layer along."""
    payload = await _describe(
        contract, adapter, _source([{"name": f"col{i:02d}"} for i in range(30)])
    )
    note = payload["note"]
    assert "30" in note
    assert "more" in note
    assert "col29" not in note


@pytest.mark.asyncio
async def test_an_empty_live_table_is_reported_as_such(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    """`DuckDBAdapter.describe_table` returns an empty schema for a table that
    does not exist rather than raising. Listing every declared column as absent
    is technically true and useless; the fact worth stating is that the table
    itself came back with nothing."""
    adapter.connection.execute("CREATE TABLE analytics.customers (id INTEGER)")
    payload = await _describe(
        contract,
        adapter,
        YamlSource.from_raw(
            {
                "metrics": [],
                "tables": [
                    {
                        "schema": "analytics",
                        "table": "subscriptions",
                        "columns": [{"name": "a"}, {"name": "b"}, {"name": "c"}],
                    }
                ],
            },
            expected_extras=[],
        ),
        table="subscriptions",
    )
    assert payload["columns"] == []
    assert "no columns" in payload["note"]
    assert "'a'" not in payload["note"]


@pytest.mark.asyncio
async def test_a_duck_typed_semantic_source_is_tolerated(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    """`SemanticSource` is a structural protocol. The reconciliation reads
    `.name` off whatever the source returns, which the protocol promises; it
    must not assume our `Column` dataclass, and it must not raise out of an
    agent's turn if a third-party source returns something thinner."""

    class _Bare:
        def __init__(self, name: str) -> None:
            self.name = name

    class _ForeignSchema:
        columns = [_Bare("id"), _Bare("column1")]

    class _ForeignSource:
        def get_table_schema(self, schema: str, table: str) -> Any:
            return _ForeignSchema()

        def get_table_schemas(self) -> dict[str, Any]:
            return {"analytics.orders": _ForeignSchema()}

        def get_metrics(self) -> list[Any]:
            return []

        def get_metric(self, name: str) -> Any:
            return None

        def search_metrics(self, query: str) -> list[Any]:
            return []

        def get_relationships(self) -> list[Any]:
            return []

        def get_relationships_for_table(self, table: str) -> list[Any]:
            return []

        def get_metric_impacts(self) -> list[Any]:
            return []

    payload = await _describe(contract, adapter, _ForeignSource())
    assert "column1" in payload["note"]
