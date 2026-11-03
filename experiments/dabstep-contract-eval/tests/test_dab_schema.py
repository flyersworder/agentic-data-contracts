"""Experiment 7's schema text: what a rater or an author sees of a built
DataAgentBench file -- each database's tables, columns and first rows, with
a database of many same-shape tables (stockmarket's 2,753 tickers)
summarised by a count and a fixed-seed sample."""

from pathlib import Path

import duckdb
import pytest
from dce.benchmarks import dab_schema


@pytest.fixture
def built(tmp_path: Path) -> Path:
    path = tmp_path / "ds.duckdb"
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA books_database")
    con.execute("CREATE TABLE books_database.books_info (title VARCHAR, n BIGINT)")
    con.execute(
        "INSERT INTO books_database.books_info VALUES "
        "('A', 1), ('B', 2), ('C', 3), ('D', 4)"
    )
    con.execute("CREATE SCHEMA trade_database")
    for i in range(60):
        kind = "BIGINT" if i < 4 else "DOUBLE"
        con.execute(f'CREATE TABLE trade_database."T{i:02d}" (d DATE, close {kind})')
        con.execute(
            f"INSERT INTO trade_database.\"T{i:02d}\" VALUES ('2020-01-02', {i})"
        )
    con.execute("CREATE TABLE trade_database.meta (k VARCHAR)")
    con.close()
    return path


def test_a_small_database_lists_every_table_with_columns_and_first_rows(built):
    text = dab_schema.schema_text(built)
    assert "## Database `books_database`" in text
    assert "CREATE TABLE books_database.books_info (" in text
    assert "  title VARCHAR," in text and "  n BIGINT" in text
    assert "title,n\nA,1\nB,2\nC,3\n" in text
    assert "D,4" not in text


def test_many_tables_of_one_shape_are_counted_and_three_shown(built):
    text = dab_schema.schema_text(built, many=50)
    assert "60 tables with the same columns" in text
    shown = [f"T{i:02d}" for i in range(60) if f"trade_database.T{i:02d} (" in text]
    assert len(shown) == 3
    assert "CREATE TABLE trade_database.meta (" in text


def test_a_type_that_varies_across_same_shape_tables_is_counted(built):
    text = dab_schema.schema_text(built, many=50)
    assert "Column close is DOUBLE in 56 of them and BIGINT in 4." in text


def test_the_sample_is_fixed_by_the_seed(built):
    assert dab_schema.schema_text(built, seed=7) == dab_schema.schema_text(
        built, seed=7
    )
    assert dab_schema.schema_text(built, seed=7) != dab_schema.schema_text(
        built, seed=8
    )


def test_long_values_are_cut(tmp_path: Path):
    path = tmp_path / "x.duckdb"
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA s")
    con.execute("CREATE TABLE s.t (v VARCHAR)")
    con.execute("INSERT INTO s.t VALUES (?)", ["x" * 500])
    con.close()
    text = dab_schema.schema_text(path, width=40)
    assert "x" * 40 + "..." in text
    assert "x" * 41 not in text


def test_names_that_are_not_plain_words_are_quoted(tmp_path: Path):
    path = tmp_path / "x.duckdb"
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA s")
    con.execute('CREATE TABLE s."BRK-A" ("Adj Close" DOUBLE, Volume BIGINT)')
    con.close()
    text = dab_schema.schema_text(path)
    assert 'CREATE TABLE s."BRK-A" (' in text
    assert '  "Adj Close" DOUBLE,' in text and "  Volume BIGINT" in text
