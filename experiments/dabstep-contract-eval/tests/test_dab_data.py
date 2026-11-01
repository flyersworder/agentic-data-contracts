"""Experiment 7's data build: one DuckDB file per DataAgentBench dataset, a
schema per logical database, values and types as in the source, checked
table by table and column by column. A synthetic dataset; no network."""

import json
import sqlite3
from pathlib import Path

import duckdb
import pytest
from dce.benchmarks import dab_data, dab_fetch

CONFIG = """db_clients:
  books_database:
    db_type: postgres
    db_name: bookreview_db
    sql_file: query_dataset/books_info.sql
  review_database:
    db_type: sqlite
    db_path: query_dataset/review_query.db
  trade_database:
    db_type: duckdb               # Database type: DuckDB
    db_path: query_dataset/trade.db
"""

DUMP = """SET client_encoding = 'UTF8';
CREATE TABLE public.books_info (
    title text,
    "MISC" text,
    rating_number bigint,
    price double precision
);
ALTER TABLE public.books_info OWNER TO postgres;
COPY public.books_info (title, "MISC", rating_number, price) FROM stdin;
A\\tB\t{"k": [1]}\t29\t8.23
\\N\tx\\ny\t\\N\t0.1
\\.
"""


def make_dataset(root: Path, name: str = "toy") -> Path:
    """A dataset folder laid out as DataAgentBench's, under `root/src`."""
    folder = root / "src" / f"query_{name}"
    data = folder / "query_dataset"
    data.mkdir(parents=True)
    (folder / "db_config.yaml").write_text(CONFIG, encoding="utf-8")
    (folder / "db_description.txt").write_text(
        "1. books_database\n2. review_database\n3. trade_database\n", encoding="utf-8"
    )
    (data / "books_info.sql").write_text(DUMP, encoding="utf-8")
    con = sqlite3.connect(data / "review_query.db")
    con.execute(
        'CREATE TABLE review (rating INTEGER, "text" TEXT, score REAL, empty REAL)'
    )
    con.executemany(
        "INSERT INTO review VALUES (?, ?, ?, ?)",
        [
            (5, "  padded  ", 0.1, None),
            (None, "caf\u00e9", 1e300, None),
            (2, "", -0.0, None),
        ],
    )
    con.execute('CREATE TABLE "Order" (Id TEXT)')
    con.execute("INSERT INTO \"Order\" VALUES ('#001')")
    con.commit()
    con.close()
    con = duckdb.connect(str(data / "trade.db"))
    con.execute(
        'CREATE TABLE "AAPL" ("Date" VARCHAR, "Adj Close" DOUBLE, "Volume" BIGINT)'
    )
    con.execute(
        "INSERT INTO \"AAPL\" VALUES ('2020-01-02', 1.5, 10), ('bad', NULL, NULL)"
    )
    con.execute('CREATE TABLE "BRK-B" ("Date" VARCHAR, "Volume" DOUBLE)')
    con.execute("INSERT INTO \"BRK-B\" VALUES ('2020-01-02', 3.0)")
    con.close()
    return folder


@pytest.fixture
def built(tmp_path):
    make_dataset(tmp_path)
    dest = tmp_path / "duckdb" / "toy.duckdb"
    entry = dab_data.build_dataset(tmp_path / "src", "toy", dest)
    return dest, entry


def test_schemas_are_the_logical_databases(built):
    dest, entry = built
    con = duckdb.connect(str(dest), read_only=True)
    tables = con.execute(
        "SELECT schema_name, table_name FROM duckdb_tables() ORDER BY 1, 2"
    ).fetchall()
    assert tables == [
        ("books_database", "books_info"),
        ("review_database", "Order"),
        ("review_database", "review"),
        ("trade_database", "AAPL"),
        ("trade_database", "BRK-B"),
    ]
    assert list(entry["schemas"]) == [
        "books_database",
        "review_database",
        "trade_database",
    ]


def test_schema_names_must_be_in_the_description(tmp_path):
    folder = make_dataset(tmp_path)
    (folder / "db_description.txt").write_text("1. books_database\n", encoding="utf-8")
    with pytest.raises(dab_data.BuildError, match="review_database"):
        dab_data.build_dataset(tmp_path / "src", "toy", tmp_path / "out.duckdb")


def test_values_and_types_are_the_sources(built):
    dest, _ = built
    con = duckdb.connect(str(dest), read_only=True)
    assert con.execute("SELECT * FROM books_database.books_info").fetchall() == [
        ("A\tB", '{"k": [1]}', 29, 8.23),
        (None, "x\ny", None, 0.1),
    ]
    assert con.execute("SELECT * FROM review_database.review").fetchall() == [
        (5, "  padded  ", 0.1, None),
        (None, "caf\u00e9", 1e300, None),
        (2, "", -0.0, None),
    ]
    types = dict(
        con.execute(
            "SELECT table_name || '.' || column_name, data_type "
            "FROM information_schema.columns"
        ).fetchall()
    )
    assert types["books_info.MISC"] == "VARCHAR"
    assert types["books_info.rating_number"] == "BIGINT"
    assert types["books_info.price"] == "DOUBLE"
    assert types["review.rating"] == "BIGINT"
    assert types["review.score"] == "DOUBLE"
    assert types["review.empty"] == "DOUBLE"  # all NULL: the declared type
    assert types["AAPL.Volume"] == "BIGINT"
    assert types["BRK-B.Volume"] == "DOUBLE"  # per table, as the source has it


def test_entry_records_rows_columns_and_digests(built):
    dest, entry = built
    review = entry["schemas"]["review_database"]["tables"]["review"]
    assert review["rows"] == 3
    assert [c[:2] for c in review["columns"]] == [
        ["rating", "BIGINT"],
        ["text", "VARCHAR"],
        ["score", "DOUBLE"],
        ["empty", "DOUBLE"],
    ]
    assert all(len(c[2]) == 64 for c in review["columns"])
    assert entry["sha256"] == dab_fetch.sha256_file(dest)
    assert entry["tables"] == 5
    assert entry["rows"] == 2 + 3 + 1 + 2 + 1
    assert len(entry["content_sha256"]) == 64


def test_digests_differ_by_value_and_by_type():
    assert dab_data.column_digests([(1,)], 1) != dab_data.column_digests([("1",)], 1)
    assert dab_data.column_digests([(1.0,)], 1) != dab_data.column_digests([(1,)], 1)
    assert dab_data.column_digests([(None,)], 1) != dab_data.column_digests([("",)], 1)
    assert dab_data.column_digests([("ab",), ("c",)], 1) != dab_data.column_digests(
        [("a",), ("bc",)], 1
    )


def test_check_finds_a_changed_value(built):
    dest, entry = built
    con = duckdb.connect(str(dest))
    con.execute("UPDATE review_database.review SET text = 'x' WHERE rating = 2")
    con.close()
    with pytest.raises(dab_data.BuildError, match="review.*text"):
        dab_data.check_built(dest, entry)


def test_check_finds_a_missing_row(built):
    dest, entry = built
    con = duckdb.connect(str(dest))
    con.execute("DELETE FROM trade_database.AAPL WHERE Date = 'bad'")
    con.close()
    with pytest.raises(dab_data.BuildError, match="AAPL.*rows"):
        dab_data.check_built(dest, entry)


def test_check_finds_an_extra_table(built):
    dest, entry = built
    con = duckdb.connect(str(dest))
    con.execute("CREATE TABLE books_database.extra (x INT)")
    con.close()
    with pytest.raises(dab_data.BuildError, match="extra"):
        dab_data.check_built(dest, entry)


def test_sqlite_value_outside_its_declared_type_stops_the_build(tmp_path):
    folder = make_dataset(tmp_path)
    con = sqlite3.connect(folder / "query_dataset" / "review_query.db")
    con.execute("INSERT INTO review VALUES ('five', 'x', 1.0, NULL)")
    con.commit()
    con.close()
    with pytest.raises(dab_data.BuildError, match="rating"):
        dab_data.build_dataset(tmp_path / "src", "toy", tmp_path / "out.duckdb")


def test_build_is_deterministic(tmp_path, built):
    dest, entry = built
    again = dab_data.build_dataset(tmp_path / "src", "toy", tmp_path / "again.duckdb")
    assert again["content_sha256"] == entry["content_sha256"]


def test_summary_and_expected_manifest(tmp_path, built):
    _, entry = built
    summary = dab_data.summary({"toy": entry})
    assert set(summary["datasets"]["toy"]) == {
        "sha256",
        "bytes",
        "tables",
        "rows",
        "content_sha256",
    }
    assert summary["dab_commit"] == dab_fetch.COMMIT
    path = tmp_path / "expected.json"
    path.write_text(json.dumps(summary), encoding="utf-8")
    dab_data.check_expected({"toy": entry}, path)
    summary["datasets"]["toy"]["content_sha256"] = "0" * 64
    path.write_text(json.dumps(summary), encoding="utf-8")
    with pytest.raises(dab_data.BuildError, match="toy"):
        dab_data.check_expected({"toy": entry}, path)


def test_large_columns_with_nulls_load(tmp_path):
    # DuckDB cannot scan a large object array of None alone; the loader masks.
    folder = make_dataset(tmp_path)
    con = sqlite3.connect(folder / "query_dataset" / "review_query.db")
    con.executemany(
        "INSERT INTO review VALUES (?, ?, ?, NULL)",
        [(None, None if i % 3 else str(i), None) for i in range(6000)],
    )
    con.commit()
    con.close()
    entry = dab_data.build_dataset(tmp_path / "src", "toy", tmp_path / "out.duckdb")
    assert entry["schemas"]["review_database"]["tables"]["review"]["rows"] == 6003
