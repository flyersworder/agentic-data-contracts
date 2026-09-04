"""``describe_table`` renders the table-level description (#89).

The semantic source's table schemas reach an agent through exactly one path --
the overlay inside ``describe_table``. A ``TableSchema.description`` that the
parser reads but no tool renders would be the same silent drop one layer along,
so this is the test that makes the field worth adding.

Adapters do not supply one today, so the rendered value comes from the semantic
source; the key is omitted entirely when neither side has one, matching how the
per-column ``description`` already behaves.
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

_DESC = "One row per authorisation attempt, not per settled transaction."


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
        """
    )
    return db


def _tool(tools: list[ToolDef], name: str) -> ToolDef:
    return next(t for t in tools if t.name == name)


def _payload(result: dict[str, Any]) -> dict[str, Any]:
    return json.loads(result["content"][0]["text"])


def _source(*, described: bool) -> YamlSource:
    table: dict[str, Any] = {
        "schema": "analytics",
        "table": "orders",
        "columns": [{"name": "id", "type": "INTEGER", "description": "Order id."}],
    }
    if described:
        table["description"] = _DESC
    return YamlSource.from_raw({"metrics": [], "tables": [table]}, expected_extras=[])


@pytest.mark.asyncio
async def test_table_description_is_rendered(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    tools = create_tools(
        contract, adapter=adapter, semantic_source=_source(described=True)
    )
    result = await _tool(tools, "describe_table").callable(
        {"schema": "analytics", "table": "orders"}
    )
    assert _payload(result)["description"] == _DESC


@pytest.mark.asyncio
async def test_the_key_is_omitted_when_no_description_exists(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    tools = create_tools(
        contract, adapter=adapter, semantic_source=_source(described=False)
    )
    result = await _tool(tools, "describe_table").callable(
        {"schema": "analytics", "table": "orders"}
    )
    assert "description" not in _payload(result)


@pytest.mark.asyncio
async def test_column_descriptions_are_unaffected(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    """The table gloss is added beside the column overlay, not instead of it."""
    tools = create_tools(
        contract, adapter=adapter, semantic_source=_source(described=True)
    )
    result = await _tool(tools, "describe_table").callable(
        {"schema": "analytics", "table": "orders"}
    )
    columns = _payload(result)["columns"]
    assert next(c for c in columns if c["name"] == "id")["description"] == "Order id."
