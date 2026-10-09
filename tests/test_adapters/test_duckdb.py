import asyncio
import threading
import time

import duckdb
import pytest

from agentic_data_contracts.adapters.base import (
    DatabaseAdapter,
    QueryResult,
    QueryTimeoutError,
    RowLimitAdapter,
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


def test_execute_timestamptz(adapter: DuckDBAdapter) -> None:
    # DuckDB imports pytz to hand a TIMESTAMP WITH TIME ZONE to Python (#135).
    result = adapter.execute("SELECT TIMESTAMPTZ '2024-01-01 00:00:00+00' AS t")
    assert result.rows[0][0].tzinfo is not None


def test_duckdb_extra_installs_pytz() -> None:
    # The test above passes here only because another extra brings pytz in;
    # the duckdb extra must declare it itself (#135).
    import tomllib
    from pathlib import Path

    pyproject = Path(__file__).resolve().parents[2] / "pyproject.toml"
    extras = tomllib.loads(pyproject.read_text(encoding="utf-8"))["project"][
        "optional-dependencies"
    ]
    assert any(dep.startswith("pytz") for dep in extras["duckdb"])


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


def test_deadline_reached_before_the_statement_starts_still_cancels_it() -> None:
    """DuckDB drops an interrupt sent while the connection is idle. A deadline
    that passes during work `execute` does first -- a subclass rewriting SQL --
    must still cancel the statement once it starts."""

    class _SlowToStart(DuckDBAdapter):
        def execute(self, sql: str) -> QueryResult:
            time.sleep(0.3)
            return super().execute(sql)

    start = time.monotonic()
    with pytest.raises(QueryTimeoutError):
        _SlowToStart(":memory:").execute_with_timeout(SLOW_SQL, 0.05)
    assert time.monotonic() - start < 5


def test_timeout_is_recognised_through_a_wrapped_interrupt() -> None:
    class _Wrapping(DuckDBAdapter):
        def execute(self, sql: str) -> QueryResult:
            try:
                return super().execute(sql)
            except Exception as e:
                raise RuntimeError(f"engine failed: {e}") from e

    with pytest.raises(QueryTimeoutError):
        _Wrapping(":memory:").execute_with_timeout(SLOW_SQL, 0.3)


def test_huge_timeout_does_not_break_the_timer(
    adapter: DuckDBAdapter, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A wait past threading.TIMEOUT_MAX raises OverflowError inside the timer
    # thread -- invisible to the caller, and the limit is never armed.
    thread_errors: list[BaseException | None] = []
    monkeypatch.setattr(
        threading, "excepthook", lambda args: thread_errors.append(args.exc_value)
    )
    result = adapter.execute_with_timeout("SELECT 1", 1e12)
    time.sleep(0.2)
    assert result.rows == [(1,)]
    assert thread_errors == []


def test_duckdb_timeout_reports_the_statement_cancelled(
    adapter: DuckDBAdapter,
) -> None:
    with pytest.raises(QueryTimeoutError) as exc_info:
        adapter.execute_with_timeout(SLOW_SQL, 0.3)
    assert exc_info.value.cancelled is True


def test_engine_error_after_the_deadline_is_not_a_timeout() -> None:
    """Only an interrupt is the timeout. A real engine error that happens to
    land after the deadline (memory_limit, a subclass's own validation) must
    reach the agent as itself, not as advice to lighten the query."""

    class _FailsLate(DuckDBAdapter):
        def execute(self, sql: str) -> QueryResult:
            time.sleep(0.3)
            raise ValueError("Out of Memory Error: failed to allocate")

    with pytest.raises(ValueError, match="Out of Memory"):
        _FailsLate(":memory:").execute_with_timeout("SELECT 1", 0.05)


# 10^10 rows: returning quickly proves nothing past the cap was materialised.
HUGE_SQL = "SELECT a.range AS x, b.range AS y FROM range(100000) a, range(100000) b"


def test_adapter_supports_row_limit(adapter: DuckDBAdapter) -> None:
    assert isinstance(adapter, RowLimitAdapter)


def test_query_result_truncated_defaults_false() -> None:
    assert QueryResult(columns=["a"], rows=[(1,)]).truncated is False


def test_execute_limited_stops_at_the_cap(adapter: DuckDBAdapter) -> None:
    start = time.monotonic()
    result = adapter.execute_limited(HUGE_SQL, 50)
    assert time.monotonic() - start < 2
    assert result.columns == ["x", "y"]
    assert len(result.rows) == 50
    assert result.row_count == 50
    assert result.truncated is True


def test_execute_limited_exact_fit_is_not_truncated(adapter: DuckDBAdapter) -> None:
    result = adapter.execute_limited("SELECT range FROM range(5)", 5)
    assert len(result.rows) == 5
    assert result.truncated is False


def test_execute_limited_small_result(adapter: DuckDBAdapter) -> None:
    result = adapter.execute_limited("SELECT id FROM analytics.orders ORDER BY id", 50)
    assert result.rows == [(1,), (2,)]
    assert result.truncated is False


def test_execute_limited_rejects_a_non_positive_cap(adapter: DuckDBAdapter) -> None:
    with pytest.raises(ValueError, match="max_rows"):
        adapter.execute_limited("SELECT 1", 0)


def test_execute_limited_honours_timeout(adapter: DuckDBAdapter) -> None:
    start = time.monotonic()
    with pytest.raises(QueryTimeoutError) as exc_info:
        adapter.execute_limited(SLOW_SQL, 10, timeout_seconds=0.3)
    assert time.monotonic() - start < 5
    assert exc_info.value.cancelled is True
    assert adapter.execute_limited("SELECT 42", 10).rows == [(42,)]


def test_connection_usable_after_a_truncated_fetch(adapter: DuckDBAdapter) -> None:
    adapter.execute_limited(HUGE_SQL, 3)
    assert adapter.execute("SELECT 42").rows == [(42,)]


def test_truncated_fetch_releases_engine_memory() -> None:
    """A truncated fetch leaves a pending streaming result, which pins the
    engine's operator state (here a 2M-row hash-join build side) until the
    next statement on the connection. `execute_limited` must release it."""
    db = DuckDBAdapter(":memory:")
    db.connection.execute(
        "CREATE TABLE big AS SELECT range AS k, range * 2 AS v FROM range(2000000)"
    )
    side = db.connection.cursor()

    def engine_bytes() -> int:
        row = side.execute(
            "SELECT sum(memory_usage_bytes) FROM duckdb_memory()"
        ).fetchone()
        assert row is not None
        return int(row[0])

    baseline = engine_bytes()
    result = db.execute_limited(
        "SELECT a.k, b.v FROM big a JOIN big b ON a.k = b.k", 10
    )
    assert result.truncated
    # Pinned, the join holds about 80 MiB over baseline; released, none.
    assert engine_bytes() - baseline < 20 * 2**20


def _setting(db: DuckDBAdapter, name: str) -> str:
    row = db.connection.execute("SELECT current_setting(?)", [name]).fetchone()
    assert row is not None
    return row[0]


def test_memory_limit_is_applied() -> None:
    db = DuckDBAdapter(":memory:", memory_limit="64MB")
    assert _setting(db, "memory_limit") == "61.0 MiB"


def test_memory_limit_default_leaves_duckdb_default() -> None:
    assert _setting(DuckDBAdapter(":memory:"), "memory_limit") == _setting(
        DuckDBAdapter(":memory:", memory_limit=None), "memory_limit"
    )


def test_runaway_query_is_an_engine_error_and_the_adapter_recovers() -> None:
    db = DuckDBAdapter(":memory:", memory_limit="50MB")
    # list() cannot spill to disk, so it hits the limit instead of paging.
    with pytest.raises(duckdb.OutOfMemoryException):
        db.execute_limited("SELECT list(range) FROM range(100000000)", 10)
    assert db.execute("SELECT 42").rows == [(42,)]


def test_invalid_memory_limit_fails_at_construction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connect = duckdb.connect

    class _ClosingSpy:
        def __init__(self, database: str) -> None:
            self._inner = connect(database)
            self.closed = False
            opened.append(self)

        def execute(self, *args: object) -> object:
            return self._inner.execute(*args)  # ty: ignore[invalid-argument-type]

        def close(self) -> None:
            self.closed = True
            self._inner.close()

    opened: list[_ClosingSpy] = []
    monkeypatch.setattr(duckdb, "connect", _ClosingSpy)
    with pytest.raises(duckdb.Error):
        DuckDBAdapter(":memory:", memory_limit="bogus")
    # The half-built adapter is unreachable, so it must not leak the
    # connection (and, for a file database, its lock).
    assert [c.closed for c in opened] == [True]
