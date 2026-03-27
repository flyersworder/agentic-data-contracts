import pytest

from agentic_data_contracts.adapters.base import (
    DatabaseAdapter,
    QueryResult,
    TableSchema,
)
from agentic_data_contracts.adapters.duckdb import DuckDBAdapter


@pytest.fixture
def adapter() -> DuckDBAdapter:
    db = DuckDBAdapter(":memory:")
    db.connection.execute(
        """
        CREATE SCHEMA IF NOT EXISTS analytics;
        CREATE TABLE analytics.orders (
            id INTEGER,
            amount DECIMAL(10,2),
            tenant_id VARCHAR
        );
        INSERT INTO analytics.orders VALUES (1, 100.00, 'acme'), (2, 200.00, 'acme');
        """
    )
    return db


def test_adapter_implements_protocol(adapter: DuckDBAdapter) -> None:
    assert isinstance(adapter, DatabaseAdapter)


def test_dialect(adapter: DuckDBAdapter) -> None:
    assert adapter.dialect == "duckdb"


def test_execute(adapter: DuckDBAdapter) -> None:
    result = adapter.execute("SELECT id, amount FROM analytics.orders ORDER BY id")
    assert isinstance(result, QueryResult)
    assert len(result.rows) == 2
    assert result.columns == ["id", "amount"]
    assert result.rows[0][0] == 1


def test_explain(adapter: DuckDBAdapter) -> None:
    result = adapter.explain("SELECT id FROM analytics.orders")
    assert result.schema_valid
    assert result.errors == []


def test_explain_returns_row_estimate(adapter: DuckDBAdapter) -> None:
    result = adapter.explain("SELECT id FROM analytics.orders")
    assert result.schema_valid
    # DuckDB should provide a row estimate
    assert result.estimated_rows is not None
    assert result.estimated_rows >= 0


def test_explain_invalid_sql(adapter: DuckDBAdapter) -> None:
    result = adapter.explain("SELECT nonexistent FROM analytics.orders")
    assert not result.schema_valid
    assert len(result.errors) > 0


def test_describe_table(adapter: DuckDBAdapter) -> None:
    schema = adapter.describe_table("analytics", "orders")
    assert isinstance(schema, TableSchema)
    assert len(schema.columns) == 3
    col_names = [c.name for c in schema.columns]
    assert "id" in col_names
    assert "amount" in col_names
    assert "tenant_id" in col_names


def test_describe_table_types(adapter: DuckDBAdapter) -> None:
    schema = adapter.describe_table("analytics", "orders")
    col_map = {c.name: c for c in schema.columns}
    assert "INTEGER" in col_map["id"].type.upper()
    assert "VARCHAR" in col_map["tenant_id"].type.upper()
