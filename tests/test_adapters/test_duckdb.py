import asyncio
import threading
import time

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


@pytest.mark.asyncio
async def test_execute_serializes_concurrent_connection_access(
    adapter: DuckDBAdapter,
) -> None:
    """The internal lock must prevent two threads from interleaving on the
    single shared DuckDB connection.

    The async tool handlers offload ``adapter.execute`` via
    ``asyncio.to_thread``, so concurrent sessions can call it from different
    worker threads at once. We instrument the underlying
    ``connection.execute`` to record peak in-flight concurrency: with the
    lock it must never exceed 1. Without the lock, the overlapping
    ``time.sleep`` windows below would push the peak above 1.
    """
    counter_lock = threading.Lock()
    active = 0
    peak = 0

    # The DuckDB C connection's ``execute`` is read-only, so wrap the whole
    # connection in a proxy that instruments ``execute`` and delegates the
    # rest (``description``/``fetchall`` live on the returned result object).
    class _TrackingConn:
        def __init__(self, real) -> None:  # type: ignore[no-untyped-def]
            self._real = real

        def execute(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            nonlocal active, peak
            with counter_lock:
                active += 1
                peak = max(peak, active)
            try:
                time.sleep(0.02)  # widen the window so an unlocked path overlaps
                return self._real.execute(*args, **kwargs)
            finally:
                with counter_lock:
                    active -= 1

        def __getattr__(self, name):  # type: ignore[no-untyped-def]
            return getattr(self._real, name)

    setattr(adapter, "connection", _TrackingConn(adapter.connection))

    results = await asyncio.gather(
        *(
            asyncio.to_thread(
                adapter.execute, "SELECT id FROM analytics.orders ORDER BY id"
            )
            for _ in range(8)
        )
    )

    assert peak == 1, f"connection access interleaved (peak concurrency={peak})"
    assert all(len(r.rows) == 2 for r in results)
