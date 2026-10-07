"""The compiler's database access: read-only, no file access, bounded.

The compiler may query the data, as an engineer writing a contract would,
but nothing else: the connection is read-only, `enable_external_access` is
off (so `read_text` and `glob` cannot reach the gold files that sit beside
LiveSQLBench's databases), results are capped at `MAX_ROWS`, and every
statement is interrupted at `QUERY_SECONDS`.
"""

from __future__ import annotations

import csv
import io
import threading
from pathlib import Path

import duckdb
import sqlglot
from sqlglot import exp

MAX_ROWS = 50
QUERY_SECONDS = 60.0


class CompileDB:
    def __init__(self, path: Path, *, init_sql: tuple[str, ...] = ()) -> None:
        self.path = path
        # pydantic-ai runs a turn's tool calls in parallel threads, and a
        # DuckDB connection holds one result at a time: without the lock one
        # query reads another's rows, and the timer interrupts the wrong one.
        self._lock = threading.Lock()
        self._con = duckdb.connect(str(path), read_only=True)
        try:
            for statement in (*init_sql, "SET enable_external_access = false"):
                self._con.execute(statement)
        except Exception:
            self._con.close()
            raise

    def close(self) -> None:
        self._con.close()

    def _execute(self, sql: str):
        timer = threading.Timer(QUERY_SECONDS, self._con.interrupt)
        timer.start()
        try:
            return self._con.execute(sql)
        finally:
            timer.cancel()

    def tables(self) -> dict[str, list[tuple[str, str]]]:
        with self._lock:
            rows = self._con.execute(
                "SELECT table_name, column_name, data_type "
                "FROM information_schema.columns WHERE table_schema = 'main' "
                "ORDER BY table_name, ordinal_position"
            ).fetchall()
        out: dict[str, list[tuple[str, str]]] = {}
        for table, column, data_type in rows:
            out.setdefault(table, []).append((column, data_type))
        return out

    def ddl(self) -> str:
        return "\n\n".join(
            f"CREATE TABLE {table} (\n"
            + ",\n".join(f"  {column} {data_type}" for column, data_type in columns)
            + "\n);"
            for table, columns in self.tables().items()
        )

    def run_sql(self, sql: str) -> str:
        try:
            with self._lock:
                cursor = self._execute(sql)
                columns = [d[0] for d in cursor.description or []]
                rows = cursor.fetchmany(MAX_ROWS + 1)
        except duckdb.Error as exc:
            return f"ERROR: {exc}"
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(columns)
        writer.writerows(rows[:MAX_ROWS])
        text = buf.getvalue().rstrip("\n")
        if len(rows) > MAX_ROWS:
            text += f"\n... truncated at {MAX_ROWS} rows"
        return text

    def check_metric(self, expr: str, source: str, *, predicate: bool) -> str | None:
        """None when the metric runs over `source` (a calculation as
        `SELECT <expr> FROM <source>`, a predicate as a `WHERE` clause that is
        BOOLEAN) and has a metric's shape (`_shape_error`); else the error."""
        return _shape_error(expr, predicate=predicate) or self._run_metric(
            expr, source, predicate=predicate
        )

    def _run_metric(self, expr: str, source: str, *, predicate: bool) -> str | None:
        sql = (
            f"SELECT * FROM {source} WHERE {expr} LIMIT 5"
            if predicate
            else f"SELECT {expr} FROM {source} LIMIT 5"
        )
        kind = "BOOLEAN"
        try:
            with self._lock:
                self._execute(sql).fetchall()
                if predicate:
                    # DuckDB coerces a number in WHERE, so a value would
                    # otherwise pass as a condition.
                    row = self._execute(
                        f"DESCRIBE SELECT {expr} FROM {source}"
                    ).fetchone()
                    kind = row[1] if row else kind
        except duckdb.Error as exc:
            return str(exc)
        if kind != "BOOLEAN":
            return f"a predicate must be a BOOLEAN condition; this is {kind}"
        return None


def _shape_error(expr: str, *, predicate: bool) -> str | None:
    """What makes a metric that runs still not a metric: a predicate that reads
    no column (a constant, which the full compile used as a placeholder for
    entries it could not encode) and a random draw. An expression sqlglot
    cannot parse is left to DuckDB, which has already run it."""
    try:
        tree = sqlglot.parse_one(f"SELECT {expr}", read="duckdb")
    except sqlglot.errors.ParseError:
        return None
    if any(tree.find_all(exp.Rand, exp.Uuid)):
        return (
            "a metric must not use random() or uuid(): it gives a new answer each run"
        )
    if not isinstance(tree, exp.Select) or len(tree.expressions) != 1:
        return "a metric is one expression"
    [value] = tree.expressions
    if not predicate and isinstance(
        value.this if isinstance(value, exp.Cast) else value, exp.Null
    ):
        return (
            "a calculation must not be NULL. If the entry cannot be encoded "
            "over this database, use cannot_encode"
        )
    # Inside a call, sqlglot reads DuckDB's JSON arrow `col->'key'` as a
    # lambda, so its column shows up as a lambda parameter.
    if predicate and not any(tree.find_all(exp.Column, exp.Lambda)):
        return (
            "a predicate must read a column of the rows; this one reads no column. "
            "If the entry cannot be encoded over this database, use cannot_encode"
        )
    return None
