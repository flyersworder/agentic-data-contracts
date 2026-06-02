"""Async tool handlers must not run blocking adapter I/O on the event loop.

The tool handlers in ``tools/factory.py`` are ``async def`` but the underlying
``DatabaseAdapter`` / ``ExplainAdapter`` methods are synchronous and may make
slow DB round-trips. Those calls must be offloaded to a worker thread (via
``asyncio.to_thread``) so a single slow query cannot stall the host's event
loop and every other coroutine sharing it.

These tests assert the blocking adapter calls execute on a *different* thread
than the event loop, which is the observable consequence of offloading.
"""

import threading
from pathlib import Path

import pytest

from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.semantic.yaml_source import YamlSource
from agentic_data_contracts.tools.factory import create_tools


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
        """
    )
    return db


@pytest.fixture
def semantic(fixtures_dir: Path) -> YamlSource:
    return YamlSource(fixtures_dir / "semantic_source.yml")


def _track_thread(adapter: DuckDBAdapter, method: str, seen: dict[str, int]) -> None:
    """Wrap ``adapter.method`` to record the thread it executes on."""
    original = getattr(adapter, method)

    def wrapper(*args, **kwargs):  # type: ignore[no-untyped-def]
        seen[method] = threading.get_ident()
        return original(*args, **kwargs)

    setattr(adapter, method, wrapper)


@pytest.mark.asyncio
async def test_run_query_offloads_execute_and_explain(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    seen: dict[str, int] = {}
    _track_thread(adapter, "execute", seen)
    _track_thread(adapter, "explain", seen)

    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "run_query")
    await tool.callable(
        {"sql": "SELECT id, amount FROM analytics.orders WHERE tenant_id = 'acme'"}
    )

    main_thread = threading.get_ident()
    assert seen["explain"] != main_thread, "EXPLAIN ran on the event-loop thread"
    assert seen["execute"] != main_thread, "execute ran on the event-loop thread"


@pytest.mark.asyncio
async def test_inspect_query_offloads_explain(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    seen: dict[str, int] = {}
    _track_thread(adapter, "explain", seen)

    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "inspect_query")
    await tool.callable(
        {"sql": "SELECT id, amount FROM analytics.orders WHERE tenant_id = 'acme'"}
    )

    assert seen["explain"] != threading.get_ident(), (
        "EXPLAIN ran on the event-loop thread"
    )


@pytest.mark.asyncio
async def test_describe_table_offloads_adapter_call(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    seen: dict[str, int] = {}
    _track_thread(adapter, "describe_table", seen)

    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "describe_table")
    await tool.callable({"schema": "analytics", "table": "orders"})

    assert seen["describe_table"] != threading.get_ident(), (
        "describe_table ran on the event-loop thread"
    )


@pytest.mark.asyncio
async def test_preview_table_offloads_execute(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    seen: dict[str, int] = {}
    _track_thread(adapter, "execute", seen)

    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "preview_table")
    await tool.callable({"schema": "analytics", "table": "orders"})

    assert seen["execute"] != threading.get_ident(), (
        "execute ran on the event-loop thread"
    )
