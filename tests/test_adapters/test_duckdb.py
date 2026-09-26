import asyncio
import threading
import time

import pytest

from agentic_data_contracts.adapters.base import (
    DatabaseAdapter,
    QueryResult,
    QueryTimeoutError,
    TableSchema,
    TimeoutAdapter,
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


# A query that runs for minutes: DuckDB cannot short-circuit a hash over a
# 10^10-row cross join the way it short-circuits count(*) over range().
SLOW_SQL = "SELECT sum(hash(a.range * b.range)) FROM range(100000) a, range(100000) b"


def test_adapter_supports_query_timeout(adapter: DuckDBAdapter) -> None:
    assert isinstance(adapter, TimeoutAdapter)


def test_execute_with_timeout_returns_fast_result(adapter: DuckDBAdapter) -> None:
    result = adapter.execute_with_timeout(
        "SELECT id FROM analytics.orders ORDER BY id", 5.0
    )
    assert [r[0] for r in result.rows] == [1, 2]


def test_execute_with_timeout_cancels_slow_query(adapter: DuckDBAdapter) -> None:
    start = time.monotonic()
    with pytest.raises(QueryTimeoutError) as exc_info:
        adapter.execute_with_timeout(SLOW_SQL, 0.5)
    assert time.monotonic() - start < 5
    assert exc_info.value.timeout_seconds == 0.5


def test_connection_usable_after_timeout(adapter: DuckDBAdapter) -> None:
    with pytest.raises(QueryTimeoutError):
        adapter.execute_with_timeout(SLOW_SQL, 0.3)
    assert len(adapter.execute("SELECT id FROM analytics.orders").rows) == 2


async def test_timeout_clock_starts_once_the_connection_is_held(
    adapter: DuckDBAdapter,
) -> None:
    """A query queued behind another must neither time out while it waits for
    the shared connection nor interrupt the query that holds it: the interrupt
    is connection-wide, so a clock started before the lock would cancel
    someone else's statement."""
    slow = asyncio.create_task(
        asyncio.to_thread(adapter.execute_with_timeout, SLOW_SQL, 1.0)
    )
    await asyncio.sleep(0.2)  # let the slow query take the connection
    fast = await asyncio.to_thread(
        adapter.execute_with_timeout, "SELECT id FROM analytics.orders", 0.3
    )
    assert len(fast.rows) == 2
    with pytest.raises(QueryTimeoutError) as exc_info:
        await slow
    assert exc_info.value.timeout_seconds == 1.0


def test_execute_with_timeout_goes_through_execute_overrides() -> None:
    """A subclass that rewrites SQL in `execute` -- the Denodo stand-in in
    test_sensitivity strips VQL's CONTEXT clause there -- must keep doing so
    when a time limit is set."""

    class _Rewriting(DuckDBAdapter):
        def execute(self, sql: str) -> QueryResult:
            return super().execute(sql.replace("NOT_SQL ", ""))

    db = _Rewriting(":memory:")
    assert db.execute_with_timeout("NOT_SQL SELECT 42", 5.0).rows == [(42,)]
    with pytest.raises(QueryTimeoutError):
        db.execute_with_timeout(f"NOT_SQL {SLOW_SQL}", 0.3)
