from pathlib import Path

import duckdb
import pytest
from dce.compile.db import MAX_ROWS, CompileDB, _shape_error


@pytest.fixture
def db(tmp_path: Path):
    path = tmp_path / "c.duckdb"
    con = duckdb.connect(str(path))
    con.execute("CREATE TABLE t AS SELECT range AS id, range % 3 AS g FROM range(120)")
    con.execute("CREATE TABLE u (k INTEGER, label VARCHAR)")
    con.close()
    (tmp_path / "secret.json").write_text('{"gold": 42}', encoding="utf-8")
    cdb = CompileDB(path, init_sql=("SET integer_division = true",))
    yield cdb
    cdb.close()


def test_tables_and_ddl(db):
    assert db.tables() == {
        "t": [("id", "BIGINT"), ("g", "BIGINT")],
        "u": [("k", "INTEGER"), ("label", "VARCHAR")],
    }
    assert "CREATE TABLE t (\n  id BIGINT,\n  g BIGINT\n);" in db.ddl()


def test_run_sql_is_csv_capped_at_the_row_limit(db):
    out = db.run_sql("SELECT id FROM t ORDER BY id").splitlines()
    assert out[0] == "id" and out[1] == "0"
    assert len(out) == 1 + MAX_ROWS + 1 and "truncated" in out[-1]
    assert db.run_sql("SELECT 7 / 2 AS x").splitlines() == [
        "x",
        "3",
    ]  # init_sql applies


def test_errors_writes_and_file_reads_come_back_as_text(db, tmp_path):
    assert db.run_sql("SELECT nope FROM t").startswith("ERROR:")
    assert db.run_sql("CREATE TABLE w (a INT)").startswith("ERROR:")
    out = db.run_sql(f"SELECT * FROM read_text('{tmp_path / 'secret.json'}')")
    assert out.startswith("ERROR:") and "42" not in out


def test_check_metric(db):
    assert db.check_metric("SUM(id)", "main.t", predicate=False) is None
    assert db.check_metric("g = 1", "t", predicate=True) is None
    assert "nope" in db.check_metric("SUM(nope)", "t", predicate=False)
    assert db.check_metric("g +", "t", predicate=True) is not None


def test_predicate_must_be_a_condition(db):
    # DuckDB coerces a number in WHERE, so a value (a KB entry that states
    # a fee, not a condition) would otherwise pass as a predicate.
    error = db.check_metric("id * 2", "t", predicate=True)
    assert error is not None and "BOOLEAN" in error
    assert db.check_metric("g = 1 AND id > 0", "t", predicate=True) is None


def test_concurrent_queries_each_get_their_own_rows(db):
    # pydantic-ai runs a turn's tool calls in parallel threads; the compiler's
    # one connection must not hand one query another query's rows.
    from concurrent.futures import ThreadPoolExecutor

    def ask(i):
        return db.run_sql(f"SELECT {i} AS x FROM range(30)").splitlines()[1:]

    with ThreadPoolExecutor(8) as pool:
        results = list(pool.map(ask, range(200)))
    assert [r for i, r in enumerate(results) if r != [str(i)] * 30] == []


def test_a_predicate_that_reads_no_column_is_rejected(db):
    # A constant passes the BOOLEAN check but says nothing about the rows: the
    # full compile turned entries it could not encode into FALSE and 1=0.
    for constant in ("TRUE", "1=0", "NULL::boolean", "(SELECT 1) = 1"):
        error = db.check_metric(constant, "t", predicate=True)
        assert error is not None and "no column" in error, constant


def test_random_is_rejected_in_any_metric(db):
    for expr, predicate in (
        ("random() < 0.5 AND g = 1", True),
        ("id * random()", False),
    ):
        error = db.check_metric(expr, "t", predicate=predicate)
        assert error is not None and "random" in error, expr


def test_a_json_arrow_inside_a_function_still_reads_a_column(db):
    # sqlglot parses `col->'key'` inside a call as a lambda, not a column.
    expr = "COALESCE(CAST(id AS VARCHAR)->'a'->>'b', '') = ''"
    assert "no column" not in (db.check_metric(expr, "t", predicate=True) or "")
    expr = "COALESCE(j->'a'->>'b', '') IN ('', 'x')"
    assert _shape_error(expr, predicate=True) is None


def test_a_calculation_that_is_null_is_rejected(db):
    for constant in ("NULL", "NULL::double", "CAST(NULL AS INTEGER)"):
        error = db.check_metric(constant, "t", predicate=False)
        assert error is not None and "NULL" in error, constant
    assert db.check_metric("INTERVAL '5 minutes'", "t", predicate=False) is None


def test_a_metric_is_one_expression(db):
    error = db.check_metric("id, g", "t", predicate=False)
    assert error is not None and "one expression" in error
    assert _shape_error("1 UNION SELECT 2", predicate=False) is not None
