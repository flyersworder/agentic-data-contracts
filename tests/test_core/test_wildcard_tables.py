"""Tests for wildcard table support in allowed_tables."""

from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.schema import (
    AllowedTable,
    DataContractSchema,
    SemanticConfig,
)


def _make_contract(tables_config: list[dict]) -> DataContract:
    allowed = [AllowedTable.model_validate(t) for t in tables_config]
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(allowed_tables=allowed),
    )
    return DataContract(schema)


def _make_adapter() -> DuckDBAdapter:
    db = DuckDBAdapter(":memory:")
    db.connection.execute("""
        CREATE SCHEMA IF NOT EXISTS analytics;
        CREATE TABLE analytics.orders (id INTEGER);
        CREATE TABLE analytics.customers (id INTEGER);
        CREATE TABLE analytics.products (id INTEGER);
        CREATE SCHEMA IF NOT EXISTS raw;
        CREATE TABLE raw.events (id INTEGER);
    """)
    return db


def test_has_wildcard_tables_true() -> None:
    dc = _make_contract([{"schema": "analytics", "tables": ["*"]}])
    assert dc.has_wildcard_tables()


def test_has_wildcard_tables_false() -> None:
    dc = _make_contract(
        [
            {"schema": "analytics", "tables": ["orders"]},
        ]
    )
    assert not dc.has_wildcard_tables()


def test_resolve_tables_expands_wildcard() -> None:
    dc = _make_contract([{"schema": "analytics", "tables": ["*"]}])
    adapter = _make_adapter()
    dc.resolve_tables(adapter)

    names = dc.allowed_table_names()
    assert "analytics.orders" in names
    assert "analytics.customers" in names
    assert "analytics.products" in names
    assert not any(n.startswith("raw.") for n in names)


def test_resolve_tables_mixed() -> None:
    dc = _make_contract(
        [
            {"schema": "analytics", "tables": ["*"]},
            {"schema": "raw", "tables": []},
        ]
    )
    adapter = _make_adapter()
    dc.resolve_tables(adapter)

    names = dc.allowed_table_names()
    assert "analytics.orders" in names
    assert not any(n.startswith("raw.") for n in names)


def test_resolve_tables_preserves_explicit() -> None:
    dc = _make_contract(
        [
            {"schema": "analytics", "tables": ["orders"]},
        ]
    )
    adapter = _make_adapter()
    dc.resolve_tables(adapter)

    names = dc.allowed_table_names()
    assert names == ["analytics.orders"]


def test_unresolved_wildcard_skipped() -> None:
    dc = _make_contract([{"schema": "analytics", "tables": ["*"]}])
    # Without calling resolve_tables, wildcard is skipped
    names = dc.allowed_table_names()
    assert names == []


def test_adapter_list_tables() -> None:
    adapter = _make_adapter()
    tables = adapter.list_tables("analytics")
    assert "orders" in tables
    assert "customers" in tables
    assert "products" in tables


def test_adapter_list_tables_empty_schema() -> None:
    adapter = _make_adapter()
    tables = adapter.list_tables("nonexistent")
    assert tables == []
