"""Experiment 7's PostgreSQL reader: plain pg_dump files (CREATE TABLE plus
COPY ... FROM stdin) and plain INSERT scripts, read without a server."""

import pytest
from dce.benchmarks import pgsql

DUMP = """--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SELECT pg_catalog.set_config('search_path', '', false);

CREATE TABLE public.books_info (
    title text,
    "MISC" text,
    rating_number bigint,
    price double precision
);


ALTER TABLE public.books_info OWNER TO postgres;

--
-- Data for Name: books_info; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.books_info (title, "MISC", rating_number, price) FROM stdin;
plain\tline one\\nline two\t29\t8.23
\\N\twith\\ttab\t\\N\t-1.5e-07
back\\\\slash \\\\N\t\\N\t0\tInfinity
\\.


--
-- PostgreSQL database dump complete
--
"""


def test_copy_line_splits_on_tabs_and_reads_null():
    assert pgsql.parse_copy_line(b"a\t\\N\t") == ["a", None, ""]


@pytest.mark.parametrize(
    ("raw", "value"),
    [
        (b"a\\tb", "a\tb"),
        (b"a\\nb", "a\nb"),
        (b"a\\rb", "a\rb"),
        (b"a\\\\b", "a\\b"),
        (b"\\\\N", "\\N"),  # a literal backslash-N, not NULL
        (b"\\b\\f\\v", "\b\f\v"),
        (b"\\101\\x42", "AB"),  # octal and hex escapes
        (b"\\303\\251", "\u00e9"),  # octal bytes forming one UTF-8 character
        (b"\\q", "q"),  # any other escaped character stands for itself
        ("caf\u00e9".encode("utf-8"), "caf\u00e9"),
    ],
)
def test_copy_escapes(raw, value):
    assert pgsql.parse_copy_line(raw) == [value]


def test_dump_tables_columns_and_rows():
    tables = pgsql.read_sql(DUMP)
    assert list(tables) == ["books_info"]
    table = tables["books_info"]
    # Unquoted names fold to lower case, quoted ones keep theirs.
    assert table.columns == [
        ("title", "text"),
        ("MISC", "text"),
        ("rating_number", "bigint"),
        ("price", "double precision"),
    ]
    assert table.rows == [
        ["plain", "line one\nline two", "29", "8.23"],
        [None, "with\ttab", None, "-1.5e-07"],
        ["back\\slash \\N", None, "0", "Infinity"],
    ]


INSERTS = """CREATE TABLE "Case" (
    Id TEXT,
    Subject TEXT,
    ClosedDate TEXT
);
INSERT INTO "Case" VALUES ('#500', 'It''s; done', NULL);
INSERT INTO "Case" VALUES ('500b', 'two
lines with \\n kept', '2023-09-30');
CREATE TABLE Issue__c (
    Id TEXT,
    Hits TEXT
);
INSERT INTO Issue__c VALUES ('a03', 5), ('a04', -1.5);
"""


def test_raw_row_counts_agree_with_the_reader_on_one_row_inserts():
    assert pgsql.count_rows(DUMP) == {"books_info": 3}
    # One row per INSERT statement; the multi-row one counts once, so the
    # build's check would stop on it rather than pass it.
    assert pgsql.count_rows(INSERTS) == {"Case": 2, "issue__c": 1}


def test_insert_script_keeps_literals_byte_for_byte():
    tables = pgsql.read_sql(INSERTS)
    assert list(tables) == ["Case", "issue__c"]
    assert tables["Case"].columns == [
        ("id", "text"),
        ("subject", "text"),
        ("closeddate", "text"),
    ]
    # Standard-conforming strings: a backslash is a backslash.
    assert tables["Case"].rows == [
        ["#500", "It's; done", None],
        ["500b", "two\nlines with \\n kept", "2023-09-30"],
    ]
    assert tables["issue__c"].rows == [["a03", "5"], ["a04", "-1.5"]]


def test_copy_column_list_must_match_the_table():
    bad = DUMP.replace(
        'COPY public.books_info (title, "MISC"', 'COPY public.books_info ("MISC", title'
    )
    with pytest.raises(ValueError, match="column"):
        pgsql.read_sql(bad)


def test_copy_row_with_wrong_field_count_is_an_error():
    bad = DUMP.replace("plain\tline one", "plain")
    with pytest.raises(ValueError, match="fields"):
        pgsql.read_sql(bad)


def test_unknown_statement_is_an_error():
    with pytest.raises(ValueError, match="unsupported"):
        pgsql.read_sql("CREATE FUNCTION f() RETURNS int AS $$ select 1 $$;")


@pytest.mark.parametrize(
    ("pg", "duck"),
    [
        ("text", "VARCHAR"),
        ("TEXT", "VARCHAR"),
        ("character varying(255)", "VARCHAR"),
        ("bigint", "BIGINT"),
        ("integer", "INTEGER"),
        ("double precision", "DOUBLE"),
        ("boolean", "BOOLEAN"),
    ],
)
def test_duckdb_types(pg, duck):
    assert pgsql.duckdb_type(pg) == duck


def test_a_type_without_a_checked_mapping_is_an_error():
    with pytest.raises(ValueError, match="numeric"):
        pgsql.duckdb_type("numeric")


@pytest.mark.parametrize(
    ("pg", "text", "value"),
    [
        ("text", "x", "x"),
        ("bigint", "-12", -12),
        ("double precision", "8.23", 8.23),
        ("boolean", "t", True),
        ("text", None, None),
    ],
)
def test_python_values_for_the_value_check(pg, text, value):
    assert pgsql.python_value(pg, text) == value
