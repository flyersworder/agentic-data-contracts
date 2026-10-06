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
* Numbers: the engines' float arithmetic differs in the last bits, which
  can flip the two-place rounding of large values, so numbers compare with
  rel_tol 1e-5 / abs_tol 0.011 after rounding.

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
            new.append(value)
        parsed.append(tuple(new))
    return [tuple(r) for r in preprocess_results(parsed)]


def _close(a, b) -> bool:
    numeric = (int, float)
    if isinstance(a, numeric) and isinstance(b, numeric) and not isinstance(a, bool):
        return math.isclose(a, b, rel_tol=1e-5, abs_tol=0.011)
    return str(a) == str(b)


def _rows_close(left: list[tuple], right: list[tuple]) -> bool:
    return len(left) == len(right) and all(
        len(x) == len(y) and all(_close(u, v) for u, v in zip(x, y))
        for x, y in zip(left, right)
    )


def _key(row: tuple) -> tuple:
    return tuple(str(v) for v in row)


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
        for statement in PG_COMPAT:
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
