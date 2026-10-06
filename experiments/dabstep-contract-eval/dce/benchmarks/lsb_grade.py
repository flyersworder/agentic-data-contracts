"""Grading for LiveSQLBench on DuckDB.

The agent's answer is the last ```sql block of its final message. It runs on
a fresh READ-ONLY connection to the pristine database, with PostgreSQL's
NULL ordering and integer division switched on, a memory limit and a time
limit, and its result is compared with the frozen PostgreSQL gold result by
the official Soft-EX rules: comments, DISTINCT and ROUND are stripped from
the query, dates normalised, numbers rounded to two places, and the rows
compared as a list (an ordered task) or a set.

Two departures from upstream, both because the gold came from PostgreSQL
and the candidate runs in DuckDB, and both established when the task set
was frozen (every frozen task's own gold SQL passes through this module):

* JSON values: DuckDB returns JSON as text, PostgreSQL as parsed objects,
  so text that parses as a JSON object or array is parsed first.
* Numbers: the engines' float arithmetic differs, which can flip the
  two-place rounding, so numbers compare within one rounding step
  (abs_tol 0.011) after rounding. A pair of floats also gets rel_tol 1e-5:
  the engines sum in different orders, and on the frozen golds that drift
  reaches 3.7e-6 relative (6.78 at 1.5e9), past one step -- six golds failed
  their own grading without it. When either side is an integer, only the
  rounding step applies, so integers that differ by one never match.
* Nested dates: upstream normalises a top-level date but not one inside a
  list or struct, which then fails to serialise. A nested date becomes the
  same `YYYY-MM-DD` text, and any other value JSON cannot hold its `str()`.

The grader's connection has no file access, as an agent's has none: the
gold results sit beside the databases, and an answer's SQL must not read
them or write anywhere.

Failure categories, all graded incorrect: `no_sql` (no block), `sql_error`
(DuckDB refused or failed the query), `timeout` and `too_large` (more than
`MAX_ROWS` rows). Any other exception --
a missing database file, say -- propagates, and `run_task` records a
`scoring_error`, never a silent `incorrect`.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import threading
import time
from datetime import date
from decimal import Decimal
from pathlib import Path

import duckdb
from vendor.livesqlbench_test_utils import (
    preprocess_results,
    remove_comments,
    remove_distinct,
    remove_round,
)

from dce.benchmark import Grade
from dce.tools import HARNESS_MEMORY_LIMIT

#: PostgreSQL semantics DuckDB can switch on per connection. The gold
#: answers come from PostgreSQL; agents (through `init_sql`) and the grader
#: alike run with these, so a query that is right in PostgreSQL stays right.
PG_COMPAT: tuple[str, ...] = (
    "SET default_null_order = 'nulls_last_on_asc_first_on_desc'",
    "SET integer_division = true",
)

#: Every connection that runs an agent's SQL, during its session or when
#: grading its answer: PostgreSQL semantics, and no file access. The gold
#: results sit beside the databases, and DuckDB's file functions
#: (`read_text`, `read_json`, `glob`) and `COPY ... TO` would reach them.
SAFE_INIT_SQL: tuple[str, ...] = (*PG_COMPAT, "SET enable_external_access = false")

#: The candidate's time limit, the same as an agent's own queries get
#: (`dce.tools.HARNESS_QUERY_SECONDS`).
GRADE_SECONDS: float = 120

#: The most rows a candidate may return. The largest gold result is 17,840
#: rows; past this cap the answer cannot be the gold, and fetching it would
#: hold Python objects the DuckDB memory limit does not bound (measured: a
#: 9M-row cartesian answer reached 3.1 GB).
MAX_ROWS = 200_000
_FETCH_CHUNK = 10_000

_SQL_BLOCK = re.compile(r"```sql[ \t]*\r?\n(.*?)```", re.DOTALL | re.IGNORECASE)


class GradeTimeout(Exception):
    """The candidate query ran past its time limit and was interrupted."""


class GradeTooLarge(Exception):
    """The candidate query returned more than `MAX_ROWS` rows."""


def extract_sql(answer: str | None) -> str | None:
    """The last ```sql block of `answer`, stripped; `None` when there is no
    non-empty one. Prose after the block does not matter."""
    blocks = _SQL_BLOCK.findall(str(answer or ""))
    if not blocks:
        return None
    sql = blocks[-1].strip()
    return sql or None


def clean(sqls: list[str]) -> list[str]:
    return remove_round(remove_distinct(remove_comments(list(sqls))))


def _jsonable(value):
    # Inside a list or struct, as upstream's json.dumps will need it.
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    if value is None or isinstance(value, (str, int, float, Decimal)):
        return value
    return str(value)


def normalise(rows) -> list[tuple]:
    parsed = []
    for row in rows:
        new = []
        for value in row:
            if isinstance(value, str) and value[:1] in "[{":
                try:
                    value = json.loads(value)
                except ValueError:
                    pass
            if isinstance(value, (dict, list, tuple)):
                value = _jsonable(value)
            new.append(value)
        parsed.append(tuple(new))
    return [tuple(r) for r in preprocess_results(parsed)]


def _is_number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _close(a, b) -> bool:
    if _is_number(a) and _is_number(b):
        # The relative band is for float drift; a count or an id is exact.
        both_floats = isinstance(a, float) and isinstance(b, float)
        return math.isclose(a, b, rel_tol=1e-5 if both_floats else 0.0, abs_tol=0.011)
    return str(a) == str(b)


def _rows_close(left: list[tuple], right: list[tuple]) -> bool:
    return len(left) == len(right) and all(
        len(x) == len(y) and all(_close(u, v) for u, v in zip(x, y))
        for x, y in zip(left, right)
    )


def _key(row: tuple) -> tuple:
    # Numbers by value, so a value one rounding step off sorts beside its
    # gold; anything else by its text.
    return tuple((0, v, "") if _is_number(v) else (1, 0, str(v)) for v in row)


def matches(gold: list[tuple], got: list[tuple], ordered: bool) -> bool:
    """True when `got` equals `gold` under Soft-EX plus the numeric
    tolerance. An empty result never matches, as upstream."""
    if not gold or not got:
        return False
    gold = [tuple(r) for r in gold]
    if ordered:
        return gold == got or _rows_close(gold, got)
    if set(gold) == set(got):
        return True
    return _rows_close(sorted(set(gold), key=_key), sorted(set(got), key=_key))


def gold_digest(rows) -> str:
    """What a row stores in `gold`: the sha256 of the frozen gold result, so
    a results file can say which gold it was graded against without
    publishing the gold."""
    text = json.dumps(rows, sort_keys=True, default=str, ensure_ascii=False)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def connect_readonly(db_path: Path, *, memory_limit: str | None = HARNESS_MEMORY_LIMIT):
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        for statement in SAFE_INIT_SQL:
            con.execute(statement)
        if memory_limit is not None:
            con.execute("SET memory_limit = ?", [memory_limit])
    except Exception:
        con.close()
        raise
    return con


def run_sql(con, sqls: list[str], *, seconds: float) -> list[tuple]:
    """Run `sqls` in order and return the last one's rows, normalised. A
    `threading.Timer` interrupts the connection at `seconds`, and the same
    deadline covers normalising, which is Python-side work the interrupt
    cannot reach. Rows are fetched in chunks, so a result past `MAX_ROWS`
    stops there rather than being held in full."""
    deadline = time.monotonic() + seconds
    timer = threading.Timer(seconds, con.interrupt)
    timer.start()
    try:
        result = None
        for sql in sqls:
            result = con.execute(sql)
        rows: list = []
        while result is not None:
            chunk = result.fetchmany(_FETCH_CHUNK)
            if not chunk:
                break
            rows.extend(chunk)
            if len(rows) > MAX_ROWS:
                raise GradeTooLarge(f"more than {MAX_ROWS} rows")
    except duckdb.InterruptException as exc:
        raise GradeTimeout(f"interrupted after {seconds}s") from exc
    finally:
        timer.cancel()
    normalised = normalise(rows)
    if time.monotonic() > deadline:
        raise GradeTimeout(f"normalising ran past {seconds}s")
    return normalised


def grade_answer(
    answer, *, db_path: Path, gold_rows, ordered: bool, seconds: float = GRADE_SECONDS
) -> Grade:
    sql = extract_sql(answer)
    if sql is None:
        return Grade("incorrect", failure="no_sql")
    con = connect_readonly(db_path)
    try:
        got = run_sql(con, clean([sql]), seconds=seconds)
    except GradeTimeout:
        return Grade("incorrect", failure="timeout")
    except GradeTooLarge:
        return Grade("incorrect", failure="too_large")
    except duckdb.Error:
        return Grade("incorrect", failure="sql_error")
    finally:
        con.close()
    gold = [tuple(r) for r in gold_rows]
    return Grade("correct" if matches(gold, got, ordered) else "incorrect")
