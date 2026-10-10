"""resources.max_query_time_seconds is enforced by the tools that execute SQL."""

import time
from typing import Any

import pytest

from agentic_data_contracts.adapters.base import (
    QueryResult,
    QueryTimeoutError,
    TableSchema,
)
from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.schema import (
    AllowedTable,
    DataContractSchema,
    ResourceConfig,
    SemanticConfig,
)
from agentic_data_contracts.core.session import ContractSession
from agentic_data_contracts.tools.factory import create_tools
from agentic_data_contracts.validation.explain import ExplainResult

# 10^10 hashed pairs: runs for minutes unless something cancels it.
SLOW_SQL = (
    "SELECT sum(hash(a.id * b.id)) FROM analytics.orders a"
    " CROSS JOIN analytics.orders b"
)
FAST_SQL = "SELECT id FROM analytics.orders WHERE id < 3"


def _contract(max_query_time_seconds: float | None) -> DataContract:
    return DataContract(
        DataContractSchema(
            name="test",
            semantic=SemanticConfig(
                allowed_tables=[
                    AllowedTable.model_validate(
                        {"schema": "analytics", "tables": ["orders"]}
                    )
                ]
            ),
            resources=ResourceConfig(
                max_query_time_seconds=max_query_time_seconds, max_retries=5
            ),
        )
    )


def _build[A: DuckDBAdapter](cls: type[A]) -> A:
    db = cls(":memory:")
    db.connection.execute(
        """
        CREATE SCHEMA analytics;
        CREATE TABLE analytics.orders AS SELECT range AS id FROM range(100000);
        """
    )
    return db


@pytest.fixture
def adapter() -> DuckDBAdapter:
    return _build(DuckDBAdapter)


def _tool(tools: list[Any], name: str) -> Any:
    return next(t for t in tools if t.name == name).callable


async def test_run_query_timeout_is_a_blocked_query(adapter: DuckDBAdapter) -> None:
    dc = _contract(0.5)
    session = ContractSession(dc)
    run_query = _tool(create_tools(dc, adapter=adapter, session=session), "run_query")

    start = time.monotonic()
    result = await run_query({"sql": SLOW_SQL})

    assert time.monotonic() - start < 5
    assert result["is_error"] is True
    assert result["_kind"] == "blocked"
    text = result["content"][0]["text"]
    assert "max_query_time_seconds" in text
    assert "0.5s" in text
    assert "filter" in text  # tells the agent how to make the query lighter
    assert "was cancelled" in text
    assert "Remaining:" in text
    assert session.retries == 1
    assert (session.blocks, session.execution_errors) == (1, 0)
    # The statement was cancelled in the database, so the connection is free.
    assert len(adapter.execute(FAST_SQL).rows) == 3


async def test_run_query_without_limit_uses_plain_execute() -> None:
    """No max_query_time_seconds declared: no timeout should reach the
    adapter, on any dispatch path. DuckDBAdapter's default row cap
    (max_result_rows=1000) routes run_query through execute_limited rather
    than execute/execute_with_timeout, so asserting execute_with_timeout was
    never called is no longer enough -- record the timeout_seconds actually
    passed to execute_limited and assert it is None (#116)."""

    seen_timeouts: list[float | None] = []

    class _NoTimeoutExpected(DuckDBAdapter):
        def execute_with_timeout(self, sql: str, timeout_seconds: float) -> QueryResult:
            raise AssertionError("no limit declared, so no timeout path")

        def execute_limited(
            self, sql: str, max_rows: int, timeout_seconds: float | None = None
        ) -> QueryResult:
            seen_timeouts.append(timeout_seconds)
            return super().execute_limited(sql, max_rows, timeout_seconds)

    run_query = _tool(
        create_tools(_contract(None), adapter=_build(_NoTimeoutExpected)),
        "run_query",
    )

    result = await run_query({"sql": FAST_SQL})

    assert "is_error" not in result
    assert '"row_count": 3' in result["content"][0]["text"]
    assert seen_timeouts == [None]


class _SlowAdapterWithoutTimeout:
    """A DatabaseAdapter with no execute_with_timeout: the tool can only stop
    waiting, not cancel the statement."""

    dialect = "duckdb"

    def __init__(self, inner: DuckDBAdapter) -> None:
        self._inner = inner

    def execute(self, sql: str) -> QueryResult:
        time.sleep(1.5)
        return self._inner.execute(sql)

    def explain(self, sql: str) -> ExplainResult:
        return self._inner.explain(sql)

    def describe_table(self, schema: str, table: str) -> TableSchema:
        return self._inner.describe_table(schema, table)

    def list_tables(self, schema: str) -> list[str]:
        return self._inner.list_tables(schema)


async def test_run_query_timeout_falls_back_for_adapters_without_support(
    adapter: DuckDBAdapter,
) -> None:
    dc = _contract(0.2)
    session = ContractSession(dc)
    run_query = _tool(
        create_tools(dc, adapter=_SlowAdapterWithoutTimeout(adapter), session=session),
        "run_query",
    )

    start = time.monotonic()
    result = await run_query({"sql": FAST_SQL})

    assert time.monotonic() - start < 1.0
    assert result["_kind"] == "blocked"
    text = result["content"][0]["text"]
    assert "max_query_time_seconds" in text
    # Nothing cancelled it, so the agent must not be told it was cancelled.
    assert "may still be running" in text
    assert "was cancelled" not in text
    assert session.retries == 1
    assert (session.blocks, session.execution_errors) == (1, 0)


class _SlowRowLimitAdapterWithoutTimeout:
    """A RowLimitAdapter (implements execute_limited) that is NOT a
    TimeoutAdapter (no execute_with_timeout): the tool can only stop waiting
    on the row-limited dispatch branch, not cancel the statement."""

    dialect = "duckdb"

    def __init__(self, inner: DuckDBAdapter) -> None:
        self._inner = inner

    def execute(self, sql: str) -> QueryResult:
        return self._inner.execute(sql)

    def execute_limited(
        self, sql: str, max_rows: int, timeout_seconds: float | None = None
    ) -> QueryResult:
        time.sleep(1.5)
        return self._inner.execute_limited(sql, max_rows, timeout_seconds)

    def explain(self, sql: str) -> ExplainResult:
        return self._inner.explain(sql)

    def describe_table(self, schema: str, table: str) -> TableSchema:
        return self._inner.describe_table(schema, table)

    def list_tables(self, schema: str) -> list[str]:
        return self._inner.list_tables(schema)


async def test_row_limited_path_falls_back_for_adapters_without_timeout_support(
    adapter: DuckDBAdapter,
) -> None:
    """The untested dispatch branch: a RowLimitAdapter that is not also a
    TimeoutAdapter still gets the caller-side deadline on the row-limited
    path. execute_limited must receive timeout_seconds=None (only a
    TimeoutAdapter is trusted to act on the real value), and a slow call is
    abandoned at the contract's limit -- surfaced as "may still be running",
    never "was cancelled" -- rather than running unbounded (#116)."""
    dc = _contract(0.2)
    session = ContractSession(dc)
    seen_timeouts: list[float | None] = []

    class _Spy(_SlowRowLimitAdapterWithoutTimeout):
        def execute_limited(
            self, sql: str, max_rows: int, timeout_seconds: float | None = None
        ) -> QueryResult:
            seen_timeouts.append(timeout_seconds)
            return super().execute_limited(sql, max_rows, timeout_seconds)

    run_query = _tool(
        create_tools(dc, adapter=_Spy(adapter), session=session), "run_query"
    )

    start = time.monotonic()
    result = await run_query({"sql": FAST_SQL})

    assert time.monotonic() - start < 1.0
    assert result["_kind"] == "blocked"
    text = result["content"][0]["text"]
    assert "max_query_time_seconds" in text
    # Nothing cancelled it, so the agent must not be told it was cancelled.
    assert "may still be running" in text
    assert "was cancelled" not in text
    assert session.retries == 1
    assert (session.blocks, session.execution_errors) == (1, 0)
    assert seen_timeouts == [None]


async def test_preview_table_honours_the_limit() -> None:
    class _AlwaysTimesOut(DuckDBAdapter):
        def execute_with_timeout(self, sql: str, timeout_seconds: float) -> QueryResult:
            raise QueryTimeoutError(timeout_seconds)

    dc = _contract(0.5)
    session = ContractSession(dc)
    preview = _tool(
        create_tools(dc, adapter=_build(_AlwaysTimesOut), session=session),
        "preview_table",
    )

    result = await preview({"schema": "analytics", "table": "orders"})

    assert result["_kind"] == "blocked"
    text = result["content"][0]["text"]
    assert "max_query_time_seconds" in text
    assert "run_query" in text
    # A timed-out preview is a block like run_query's: it counts against
    # max_retries, so repeated previews cannot pile up abandoned scans.
    assert (session.blocks, session.retries) == (1, 1)


def test_warns_at_wiring_when_the_adapter_cannot_cancel(
    adapter: DuckDBAdapter,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from agentic_data_contracts.tools import factory

    # The warning is deduplicated per adapter class across the process.
    monkeypatch.setattr(factory, "_WARNED_UNCANCELLABLE", set())
    create_tools(_contract(30), adapter=_SlowAdapterWithoutTimeout(adapter))
    assert "cannot cancel the statement" in caplog.text

    caplog.clear()
    monkeypatch.setattr(factory, "_WARNED_UNCANCELLABLE", set())
    create_tools(_contract(30), adapter=adapter)
    create_tools(_contract(None), adapter=_SlowAdapterWithoutTimeout(adapter))
    assert "max_query_time_seconds" not in caplog.text


async def test_fallback_does_not_mistake_a_driver_timeout_for_the_limit(
    adapter: DuckDBAdapter,
) -> None:
    """A driver's own TimeoutError (socket.timeout is an alias) is an engine
    failure, not the contract's limit: telling the agent to lighten a correct
    query would send it rewriting the wrong thing."""

    class _DriverTimesOut(_SlowAdapterWithoutTimeout):
        def execute(self, sql: str) -> QueryResult:
            raise TimeoutError("read timed out")

    run_query = _tool(
        create_tools(_contract(30), adapter=_DriverTimesOut(adapter)), "run_query"
    )

    text = (await run_query({"sql": FAST_SQL}))["content"][0]["text"]

    assert "read timed out" in text
    assert "max_query_time_seconds" not in text


def test_wiring_warning_is_logged_once_per_adapter_class(
    adapter: DuckDBAdapter,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """create_pydantic_ai_toolset rebuilds the tools on every agent run; the
    warning must not repeat with it."""
    from agentic_data_contracts.tools import factory

    monkeypatch.setattr(factory, "_WARNED_UNCANCELLABLE", set())
    for _ in range(3):
        create_tools(_contract(30), adapter=_SlowAdapterWithoutTimeout(adapter))
    assert caplog.text.count("cannot cancel the statement") == 1


def test_contract_middleware_warns_it_does_not_enforce_the_limit(
    caplog: pytest.LogCaptureFixture,
) -> None:
    from agentic_data_contracts.tools.middleware import contract_middleware

    contract_middleware(_contract(30))
    assert "max_query_time_seconds" in caplog.text

    caplog.clear()
    contract_middleware(_contract(None))
    assert "max_query_time_seconds" not in caplog.text


def test_wiring_warning_repeats_for_a_different_limit(
    adapter: DuckDBAdapter,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from agentic_data_contracts.tools import factory

    monkeypatch.setattr(factory, "_WARNED_UNCANCELLABLE", set())
    create_tools(_contract(30), adapter=_SlowAdapterWithoutTimeout(adapter))
    create_tools(_contract(60), adapter=_SlowAdapterWithoutTimeout(adapter))
    assert caplog.text.count("cannot cancel the statement") == 2
