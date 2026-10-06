from pathlib import Path

import duckdb
import pytest
from dce.benchmark import Grade
from dce.benchmarks.lsb_grade import (
    extract_sql,
    gold_digest,
    grade_answer,
    matches,
)


@pytest.fixture
def db(tmp_path: Path) -> Path:
    path = tmp_path / "g.duckdb"
    con = duckdb.connect(str(path))
    con.execute(
        "CREATE TABLE t AS SELECT * FROM (VALUES "
        "(1, 'a', 10.004, NULL::JSON), (2, 'b', NULL, '{\"k\": [1, 2]}'), "
        "(3, 'c', 7.0, NULL)) v(id, name, amount, doc)"
    )
    con.close()
    return path


def _answer(sql: str) -> str:
    return f"Here is the query.\n\n```sql\n{sql}\n```"


def test_extract_takes_the_last_sql_block_and_ignores_prose_after_it():
    text = "```sql\nSELECT 1\n```\nthen\n```SQL\nSELECT 2;\n```\nThat is all."
    assert extract_sql(text) == "SELECT 2;"
    assert extract_sql("no block here") is None
    assert extract_sql("```sql\n   \n```") is None
    assert extract_sql("```python\nprint(1)\n```") is None


def test_no_block_is_no_sql(db):
    grade = grade_answer("The answer is 3.", db_path=db, gold_rows=[[3]], ordered=False)
    assert grade == Grade("incorrect", failure="no_sql")


def test_an_engine_error_is_sql_error(db):
    grade = grade_answer(
        _answer("SELECT nope FROM t"), db_path=db, gold_rows=[[1]], ordered=False
    )
    assert grade == Grade("incorrect", failure="sql_error")


def test_a_runaway_query_is_a_timeout(db):
    grade = grade_answer(
        _answer("SELECT count(*) FROM range(100000000000) a"),
        db_path=db,
        gold_rows=[[1]],
        ordered=False,
        seconds=0.2,
    )
    assert grade == Grade("incorrect", failure="timeout")


def test_order_counts_only_when_the_task_is_ordered(db):
    gold = [["a"], ["b"], ["c"]]
    desc = _answer("SELECT name FROM t ORDER BY id DESC")
    assert grade_answer(desc, db_path=db, gold_rows=gold, ordered=False) == Grade(
        "correct"
    )
    assert grade_answer(desc, db_path=db, gold_rows=gold, ordered=True) == Grade(
        "incorrect"
    )


def test_postgres_integer_division_and_null_order_apply(db):
    # integer_division: 7 / 2 is 3, as in PostgreSQL.
    assert grade_answer(
        _answer("SELECT 7 / 2"), db_path=db, gold_rows=[[3]], ordered=False
    ) == Grade("correct")
    # DESC puts NULLs first, as in PostgreSQL.
    gold = [[None], [10.0], [7.0]]
    got = _answer("SELECT amount FROM t ORDER BY amount DESC")
    assert grade_answer(got, db_path=db, gold_rows=gold, ordered=True) == Grade(
        "correct"
    )


def test_round_and_distinct_are_stripped_and_numbers_compare_at_two_places(db):
    # ROUND(amount, 0) is stripped to `amount`, which rounds to 10.0 at two places.
    got = _answer("SELECT DISTINCT ROUND(amount, 0) FROM t WHERE id = 1")
    assert grade_answer(got, db_path=db, gold_rows=[[10.0]], ordered=False) == Grade(
        "correct"
    )


def test_json_text_is_parsed_before_comparing(db):
    got = _answer("SELECT doc FROM t WHERE id = 2")
    assert grade_answer(
        got, db_path=db, gold_rows=[['{"k": [1, 2]}']], ordered=False
    ) == Grade("correct")


def test_extra_or_reordered_columns_are_wrong_as_in_the_official_rule(db):
    gold = [["a", 1]]
    assert grade_answer(
        _answer("SELECT id, name FROM t WHERE id = 1"),
        db_path=db,
        gold_rows=gold,
        ordered=False,
    ) == Grade("incorrect")
    assert grade_answer(
        _answer("SELECT name, id, 0 FROM t WHERE id = 1"),
        db_path=db,
        gold_rows=gold,
        ordered=False,
    ) == Grade("incorrect")


def test_an_empty_result_never_matches():
    assert matches([], [], ordered=False) is False
    assert matches([(1,)], [], ordered=False) is False


def test_the_grader_opens_the_database_read_only(db):
    got = _answer("DELETE FROM t; SELECT count(*) FROM t")
    assert (
        grade_answer(got, db_path=db, gold_rows=[[0]], ordered=False).failure
        == "sql_error"
    )
    con = duckdb.connect(str(db), read_only=True)
    assert con.execute("SELECT count(*) FROM t").fetchone() == (3,)
    con.close()


def test_gold_digest_is_stable_and_does_not_contain_the_values():
    digest = gold_digest([["Ann", 120.5]])
    assert digest == gold_digest([["Ann", 120.5]])
    assert digest != gold_digest([["Ann", 120.51]])
    assert len(digest) == 64 and "Ann" not in digest


def test_a_result_past_the_row_cap_is_too_large_not_fetched_in_full(db, monkeypatch):
    import dce.benchmarks.lsb_grade as lsb_grade

    monkeypatch.setattr(lsb_grade, "MAX_ROWS", 50)
    grade = grade_answer(
        _answer("SELECT * FROM range(1000)"), db_path=db, gold_rows=[[1]], ordered=False
    )
    assert grade == Grade("incorrect", failure="too_large")
    # At the cap is still graded.
    assert grade_answer(
        _answer("SELECT * FROM range(50)"),
        db_path=db,
        gold_rows=[[i] for i in range(50)],
        ordered=False,
    ) == Grade("correct")


def test_the_time_limit_covers_normalising_the_result(db, monkeypatch):
    import time

    import dce.benchmarks.lsb_grade as lsb_grade

    def slow(rows):
        time.sleep(0.5)
        return [tuple(r) for r in rows]

    monkeypatch.setattr(lsb_grade, "normalise", slow)
    grade = grade_answer(
        _answer("SELECT 1"), db_path=db, gold_rows=[[1]], ordered=False, seconds=0.2
    )
    assert grade == Grade("incorrect", failure="timeout")


def test_the_grader_has_no_file_access(db, tmp_path):
    # The gold results sit beside the databases; an answer must not read them,
    # nor write anywhere on the grading host.
    secret = tmp_path / "gold.json"
    secret.write_text('[{"x": 42}]')
    read = _answer(f"SELECT x FROM read_json_auto('{secret}')")
    assert grade_answer(read, db_path=db, gold_rows=[[42]], ordered=False) == Grade(
        "incorrect", failure="sql_error"
    )
    out = tmp_path / "written.csv"
    write = _answer(f"COPY (SELECT 1) TO '{out}'; SELECT 42")
    assert (
        grade_answer(write, db_path=db, gold_rows=[[42]], ordered=False).failure
        == "sql_error"
    )
    assert not out.exists()


def test_integers_compare_exactly():
    assert matches([(100000,)], [(100001,)], ordered=False) is False
    assert matches([(100000,)], [(100000,)], ordered=False) is True


def test_the_tolerance_is_one_rounding_step_and_a_float_drift_band():
    # A last-bit difference can flip the two-place rounding: one step passes.
    assert matches([(0.13,)], [(0.12,)], ordered=True) is True
    assert matches([(123456789012.35,)], [(123456789012.34,)], ordered=True) is True
    # A large value off by more than one step does not when either side is
    # an integer ...
    assert matches([(1000000.0,)], [(1000009,)], ordered=True) is False
    # ... nor a float pair outside the drift band.
    assert matches([(12345.67,)], [(12346.0,)], ordered=False) is False


def test_float_pairs_absorb_the_measured_engine_drift():
    """PostgreSQL and DuckDB sum floats in different orders. Measured on the
    frozen golds: up to 3.7e-6 relative (0.5 at 1.9e5, 6.78 at 1.5e9), past
    one rounding step. Six golds failed their own grading without this."""
    assert matches([(1510000000.12,)], [(1510000006.9,)], ordered=True) is True
    assert matches([(190000.25,)], [(190000.75,)], ordered=True) is True
    assert matches([(190000.25,)], [(190003.0,)], ordered=True) is False


def test_unordered_rows_pair_numbers_by_value_not_by_their_text():
    # As text, 10.0 sorts before 5.0 but 9.99 after it: pairing by text would
    # set 10.0 against 5.0 and fail an answer one rounding step away.
    assert matches([(10.0,), (5.0,)], [(9.99,), (5.0,)], ordered=False) is True


def test_a_nested_date_is_graded_not_raised(db):
    got = _answer("SELECT [DATE '2024-01-02'], {'d': DATE '2024-01-03'}")
    gold = [['["2024-01-02"]', '{"d": "2024-01-03"}']]
    assert grade_answer(got, db_path=db, gold_rows=gold, ordered=False) == Grade(
        "correct"
    )
    assert grade_answer(
        got, db_path=db, gold_rows=[["x", "y"]], ordered=False
    ) == Grade("incorrect")
