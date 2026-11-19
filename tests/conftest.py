from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def fixtures_dir() -> Path:
    return FIXTURES_DIR


@pytest.fixture(autouse=True)
def _close_duckdb_connections(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Close every DuckDB connection a test opened, at its teardown.

    It sees only connections opened after it starts, so a module- or
    session-scoped fixture (set up first) must close its own connection.

    A DuckDB connection holds native worker threads until it is closed or
    freed (about 50 per connection from DuckDB 2.0). Adapters built by
    fixtures end up in reference cycles, so only the cyclic GC frees them, and
    it is driven by Python allocations, not threads: across the suite the
    threads piled up to the per-process cap ("can't start new thread").
    """
    try:
        import duckdb
    except ImportError:
        yield
        return
    opened: list[duckdb.DuckDBPyConnection] = []
    connect = duckdb.connect

    def _tracking_connect(*args: Any, **kwargs: Any) -> duckdb.DuckDBPyConnection:
        conn = connect(*args, **kwargs)
        opened.append(conn)
        return conn

    monkeypatch.setattr(duckdb, "connect", _tracking_connect)
    yield
    for conn in opened:
        conn.close()
