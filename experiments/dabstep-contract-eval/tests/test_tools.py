from pathlib import Path

import duckdb
import pytest
from dce.tools import _BoundedDuckDBAdapter, _ungoverned_tools

# DuckDB's `/` is float division unless `integer_division` is set, which
# makes it a one-statement probe for "did init_sql run on this connection".
INTEGER_DIVISION = ("SET integer_division = true",)


@pytest.fixture
def db(tmp_path: Path) -> Path:
    path = tmp_path / "t.duckdb"
    con = duckdb.connect(str(path))
    con.execute("CREATE TABLE t AS SELECT 7 AS x")
    con.close()
    return path


def _tool(tools, name: str):
    return next(t for t in tools if t.name == name)


def test_bounded_adapter_runs_init_sql_on_its_connection(db):
    adapter = _BoundedDuckDBAdapter(db, init_sql=INTEGER_DIVISION)
    try:
        result = adapter.execute_limited("SELECT x / 2 AS h FROM t", 5)
    finally:
        adapter.connection.close()
    assert result.rows[0][0] == 3


def test_bounded_adapter_without_init_sql_is_unchanged(db):
    adapter = _BoundedDuckDBAdapter(db)
    try:
        result = adapter.execute_limited("SELECT x / 2 AS h FROM t", 5)
    finally:
        adapter.connection.close()
    assert result.rows[0][0] == 3.5


def test_ungoverned_execute_sql_runs_init_sql(db):
    tools = _ungoverned_tools(db, init_sql=INTEGER_DIVISION)
    out = _tool(tools, "execute_sql").function("SELECT x / 2 AS h FROM t")
    assert out.splitlines() == ["h", "3"]


def test_a_failing_init_sql_closes_the_connection_and_raises(db):
    with pytest.raises(duckdb.Error):
        _BoundedDuckDBAdapter(db, init_sql=("SET no_such_setting = 1",))
    # The file is not left locked by a half-built adapter.
    duckdb.connect(str(db)).close()
